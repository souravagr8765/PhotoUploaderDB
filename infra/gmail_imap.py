"""
Gmail IMAP Module: Checks Gmail inbox for replies to album creation notification emails
and extracts shared Google Photos album URLs from the reply body.

Uses the same credentials as the SMTP config in config.yaml.
"""

import imaplib
import email
import re
import os
from email.header import decode_header
import infra.logger as logger
from infra.config_loader import get_config


# Regex patterns for Google Photos album URLs
GOOGLE_PHOTOS_URL_PATTERNS = [
    re.compile(r'https?://photos\.app\.goo\.gl/\S+', re.IGNORECASE),
    re.compile(r'https?://photos\.google\.com/(?:share|album|u/\d+/album)/\S+', re.IGNORECASE),
    re.compile(r'https?://photos\.google\.com/\S+', re.IGNORECASE),
]


def _decode_email_header(header_value: str) -> str:
    """Decode an email header that may be encoded (e.g., with =?UTF-8?)."""
    if not header_value:
        return ""
    decoded_parts = decode_header(header_value)
    parts = []
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            try:
                parts.append(part.decode(encoding or "utf-8", errors="replace"))
            except (LookupError, UnicodeDecodeError):
                parts.append(part.decode("utf-8", errors="replace"))
        else:
            parts.append(part)
    return " ".join(parts)


def _get_email_body(msg: email.message.Message) -> str:
    """Extract the plain text body from an email message."""
    body = ""

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain":
                try:
                    payload = part.get_payload(decode=True)
                    if payload:
                        charset = part.get_content_charset() or "utf-8"
                        body += payload.decode(charset, errors="replace")
                except Exception:
                    pass
            elif content_type == "text/html":
                # Only fall back to HTML if we haven't found a plain text part
                if not body:
                    try:
                        payload = part.get_payload(decode=True)
                        if payload:
                            charset = part.get_content_charset() or "utf-8"
                            body += payload.decode(charset, errors="replace")
                    except Exception:
                        pass
    else:
        try:
            payload = msg.get_payload(decode=True)
            if payload:
                charset = msg.get_content_charset() or "utf-8"
                body = payload.decode(charset, errors="replace")
        except Exception:
            pass

    return body


def _extract_google_photos_url(text: str) -> str | None:
    """Extract the first Google Photos URL found in text."""
    if not text:
        return None

    for pattern in GOOGLE_PHOTOS_URL_PATTERNS:
        match = pattern.search(text)
        if match:
            url = match.group(0)
            # Clean up trailing punctuation that might have been captured
            url = url.rstrip(".,;:!?\"')]>}")
            return url

    return None


def check_for_album_url_reply(message_id: str, timeout_seconds: int = 30) -> str | None:
    """
    Connect to Gmail via IMAP and search for replies to the email with the given Message-ID.
    Returns the first Google Photos album URL found in the reply, or None.

    Uses the SMTP credentials from config.yaml (user/password).
    """
    smtp_cfg = get_config("smtp", {})
    if not smtp_cfg:
        logger.warning("⚠️ No SMTP config found — cannot check Gmail via IMAP.")
        return None

    user = smtp_cfg.get("user")
    password = smtp_cfg.get("password")

    if not user or not password:
        logger.warning("⚠️ Missing SMTP user or password — cannot check Gmail via IMAP.")
        return None

    url, _ = _search_gmail_for_reply(user, password, message_id, timeout_seconds)
    return url


def check_for_album_url(trip_name: str, message_id: str | None = None, timeout_seconds: int = 30) -> tuple:
    """
    Unified function to find a shared album URL from Gmail replies.

    If message_id is provided:
        Search for replies to that specific email (fast path).
    If message_id is None:
        Search for the original notification email by subject, extract its Message-ID,
        then search for replies to it (fallback for existing trips).

    Returns: (album_url, found_message_id)
        - album_url: the shared Google Photos URL, or None if not found
        - found_message_id: the Message-ID of the original notification email (useful when
          message_id was None and we discovered it), or None
    """
    smtp_cfg = get_config("smtp", {})
    if not smtp_cfg:
        logger.warning("⚠️ No SMTP config found — cannot check Gmail via IMAP.")
        return None, None

    user = smtp_cfg.get("user")
    password = smtp_cfg.get("password")

    if not user or not password:
        logger.warning("⚠️ Missing SMTP user or password — cannot check Gmail via IMAP.")
        return None, None

    return _search_gmail_for_album_url(user, password, trip_name, message_id, timeout_seconds)


def _search_gmail_for_reply(user: str, password: str, message_id: str, timeout: int = 30) -> tuple:
    """
    Connect to Gmail IMAP, search for a reply to the given Message-ID,
    and extract a Google Photos URL from the reply body.

    Returns: (url_or_None, message_id_or_None)
    """
    import socket
    original_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout)

    mail = None
    try:
        mail = _imap_connect(user, password, timeout)
        if not mail:
            return None, None

        url, _ = _search_replies_in_mailbox(mail, message_id)
        return url, message_id

    except Exception as e:
        logger.error(f"❌ IMAP error: {e}")
        return None, None
    finally:
        if mail:
            try:
                mail.logout()
            except Exception:
                pass
        socket.setdefaulttimeout(original_timeout)


def _search_gmail_for_album_url(user: str, password: str, trip_name: str, message_id: str | None = None, timeout: int = 30) -> tuple:
    """
    Internal implementation: search Gmail for an album URL, with or without a stored Message-ID.

    If message_id is provided: search replies to that specific email.
    If message_id is None: find the original notification email by subject,
                           then search replies to it.

    Returns: (album_url, found_message_id)
    """
    import socket
    original_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout)

    mail = None
    try:
        mail = _imap_connect(user, password, timeout)
        if not mail:
            return None, None

        # Step 1: Determine the Message-ID to search for
        search_msg_id = message_id

        if not search_msg_id:
            # No stored Message-ID — find the original notification email by subject
            logger.info(f"🔍 No stored Message-ID for '{trip_name}'. Searching Gmail by subject...")
            found_msg_id = _find_original_message_id(mail, trip_name)
            if not found_msg_id:
                logger.info(f"📭 Could not find original notification email for '{trip_name}'.")
                return None, None
            search_msg_id = found_msg_id
            logger.info(f"📧 Found original email Message-ID: {search_msg_id[:60]}...")

        # Step 2: Search for replies to that email
        url, _ = _search_replies_in_mailbox(mail, search_msg_id)

        # Return the URL and the Message-ID (so caller can persist the msg_id for future runs)
        return url, search_msg_id

    except Exception as e:
        logger.error(f"❌ IMAP error: {e}")
        return None, None
    finally:
        if mail:
            try:
                mail.logout()
            except Exception:
                pass
        socket.setdefaulttimeout(original_timeout)


def _imap_connect(user: str, password: str, timeout: int = 30) -> imaplib.IMAP4_SSL | None:
    """Connect to Gmail IMAP and return the connection object."""
    try:
        logger.info(f"📧 Connecting to Gmail IMAP...")
        mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        mail.login(user, password)
        mail.select("INBOX", readonly=True)
        logger.info("📧 Connected to Gmail INBOX.")
        return mail
    except imaplib.IMAP4.error as e:
        logger.error(f"❌ IMAP authentication/connection error: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ IMAP connection error: {e}")
        return None


def _find_original_message_id(mail: imaplib.IMAP4_SSL, trip_name: str) -> str | None:
    """
    Search the mailbox for the original album creation notification email by subject.
    Returns its Message-ID, or None if not found.

    The email subject will be one of:
      - "📸 New Trip Album Created: {trip_name}"
      - "🔔 Album Split Notification: {trip_name}"
    """
    # Search with a broad subject match, then filter by trip name
    search_terms = '(OR SUBJECT "New Trip Album Created" SUBJECT "Album Split Notification")'
    typ, data = mail.uid("SEARCH", None, search_terms)

    if typ != "OK" or not data or not data[0]:
        logger.info(f"📭 No album notification emails found in inbox.")
        return None

    uid_list = data[0].split()
    logger.info(f"📬 Found {len(uid_list)} album notification emails. Checking for trip '{trip_name}'...")

    for uid in reversed(uid_list):  # Check newest first
        uid = uid.decode() if isinstance(uid, bytes) else uid
        typ, msg_data = mail.uid("FETCH", uid, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID SUBJECT)])")

        if typ != "OK" or not msg_data or not msg_data[0]:
            continue

        raw_header = msg_data[0][1]
        if isinstance(raw_header, bytes):
            header_text = raw_header.decode("utf-8", errors="replace")
        else:
            header_text = str(raw_header)

        # Extract Message-ID
        msg_id = None
        for line in header_text.split("\r\n"):
            if line.lower().startswith("message-id:"):
                msg_id = line.split(":", 1)[1].strip()

        # Extract Subject and check for trip name
        subject = None
        for line in header_text.split("\r\n"):
            if line.lower().startswith("subject:"):
                subject = _decode_email_header(line.split(":", 1)[1].strip())
                break

        if subject and trip_name.lower() in subject.lower():
            logger.info(f"✅ Found notification email for '{trip_name}' — Message-ID: {msg_id[:50] if msg_id else 'N/A'}...")
            return msg_id

    logger.info(f"📭 No notification email found specifically for trip '{trip_name}'.")
    return None


def _search_replies_in_mailbox(mail: imaplib.IMAP4_SSL, message_id: str) -> tuple:
    """
    Search the currently selected mailbox for replies to a given Message-ID.
    Returns: (first_album_url_found, message_id) or (None, message_id)
    """
    # Gmail's IMAP stores Message-ID in the References/In-Reply-To headers.
    # A reply email will have In-Reply-To pointing to the original Message-ID.
    # We search both In-Reply-To and References to cover different mail clients.
    search_criteria = f'(OR (IN-REPLY-TO "{message_id}") (REFERENCES "{message_id}"))'
    typ, data = mail.uid("SEARCH", None, search_criteria)

    if typ != "OK" or not data or not data[0]:
        logger.info("📭 No reply found yet for this album notification.")
        return None, message_id

    # Get the list of UIDs found
    uid_list = data[0].split()
    if not uid_list:
        return None, message_id

    logger.info(f"📬 Found {len(uid_list)} potential reply/replies. Checking for album URL...")

    for uid in uid_list:
        uid = uid.decode() if isinstance(uid, bytes) else uid
        typ, msg_data = mail.uid("FETCH", uid, "(BODY[] FLAGS)")

        if typ != "OK" or not msg_data or not msg_data[0]:
            continue

        # Parse the email
        raw_email = msg_data[0][1]
        if isinstance(raw_email, bytes):
            try:
                msg = email.message_from_bytes(raw_email)
            except Exception:
                msg = email.message_from_string(raw_email.decode("utf-8", errors="replace"))
        else:
            msg = email.message_from_string(str(raw_email))

        # Extract body and look for URL
        body_text = _get_email_body(msg)
        url = _extract_google_photos_url(body_text)

        if url:
            logger.info(f"✅ Found shared album URL in reply: {url}")
            return url, message_id

    logger.info("📭 No Google Photos URL found in replies.")
    return None, message_id
