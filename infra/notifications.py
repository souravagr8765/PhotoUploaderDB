import os
import smtplib
import yaml
import requests
import json
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from infra.config_loader import get_config

def load_config():
    # Deprecated: use get_config or infra.config_loader directly.
    # Kept for backward compatibility if other modules use it.
    from infra.config_loader import Config
    return Config.load()

def send_notification(subject, body, is_html=False):
    """
    Sends a notification via all enabled channels (SMTP, Telegram).
    Returns the Message-ID of the sent email (if SMTP was used), or None.
    """
    config = load_config()
    if not config:
        return None

    message_id = None

    # 1. SMTP
    smtp_cfg = config.get('smtp', {})
    if smtp_cfg.get('enabled', False):
        message_id = _send_email(subject, body, smtp_cfg, is_html)

    # 2. Telegram
    tg_cfg = config.get('telegram', {})
    if tg_cfg.get('enabled', False):
        _send_telegram(f"<b>{subject}</b>\n\n{body}", tg_cfg)

    return message_id

def _send_email(subject, body, smtp_cfg, is_html):
    required_keys = ['server', 'port', 'user', 'password', 'recipient']
    if not all(key in smtp_cfg for key in required_keys):
        return None

    msg = MIMEMultipart()
    msg['From'] = smtp_cfg['user']
    msg['To'] = smtp_cfg['recipient']
    msg['Subject'] = subject

    # Generate a unique Message-ID so we can track replies to this email
    domain = smtp_cfg['user'].split('@')[-1] if '@' in smtp_cfg['user'] else 'localhost'
    timestamp = str(int(time.time() * 1000000))
    msg_id_value = f"<{timestamp}.{id(msg)}@{domain}>"
    msg['Message-ID'] = msg_id_value

    content_type = 'html' if is_html else 'plain'
    msg.attach(MIMEText(body, content_type))

    try:
        server = smtplib.SMTP(smtp_cfg['server'], smtp_cfg['port'])
        server.starttls()
        server.login(smtp_cfg['user'], smtp_cfg['password'])
        server.send_message(msg)
        server.quit()
        return msg_id_value
    except Exception:
        return None

def _send_telegram(message, tg_cfg):
    token = tg_cfg.get('token')
    chat_ids = tg_cfg.get('chat_ids', [])

    if not token or not chat_ids:
        return

    for chat_id in chat_ids:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        try:
            requests.post(url, json=payload, timeout=10)
        except Exception:
            pass
