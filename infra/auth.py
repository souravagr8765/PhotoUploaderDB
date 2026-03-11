import os
import json
import smtplib
from email.message import EmailMessage
import pickle
import subprocess
import socket
import time
from google.auth.transport.requests import Request
import infra.logger as logger

# Global Config references
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "Data")
ACTIVE_ACC_FILE = os.path.join(DATA_DIR, "active_account.txt")

_raw_accounts = os.getenv("GOOGLE_ACCOUNTS", "")
ACCOUNTS = [a.strip() for a in _raw_accounts.split(",") if a.strip()]
if not ACCOUNTS:
    import infra.logger as _startup_logger
    _startup_logger.error("❌ GOOGLE_ACCOUNTS is not set in .env. Please add a comma-separated list of Google account emails.")

smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
smtp_port = os.getenv("SMTP_PORT", "587")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")
APP_PASSWORD = os.getenv("APP_PASSWORD")

def wait_for_internet(timeout=5, retry_interval=10):
    first_attempt = True
    while True:
        try:
            socket.create_connection(("8.8.8.8", 53), timeout=timeout)
            if not first_attempt:
                logger.info("🌐 Internet Connection Restored.")
            return
        except OSError:
            if first_attempt:
                logger.warning("🌐 No internet. Waiting...")
                first_attempt = False
            time.sleep(retry_interval)

def send_email(subject, body, device_name="Unknown_Device"):
    if not SENDER_EMAIL or not APP_PASSWORD:
        logger.warning("⚠️ Email not configured. Skipping email.")
        return

    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECEIVER_EMAIL

    try:
        server = smtplib.SMTP(smtp_server, int(smtp_port))
        server.starttls()
        server.login(SENDER_EMAIL, APP_PASSWORD)
        server.send_message(msg)
        server.quit()
        logger.info(f"📧 Email sent: {subject}")
    except Exception as e:
        logger.error(f"❌ Failed to send email: {e}")

def get_storage_usage(remote):
    wait_for_internet()
    try:
        result = subprocess.run(['rclone', 'about', f'{remote}:', '--json'], capture_output=True, text=True, shell=False, check=True)
        data = json.loads(result.stdout)
        used = data.get("used", 0) + data.get("other", 0)
        total = data.get("total", 1)
        return (used / total) * 100
    except Exception:
        return 0

def get_active_account_info():
    idx = 0
    if os.path.exists(ACTIVE_ACC_FILE):
        with open(ACTIVE_ACC_FILE, 'r') as f:
            try: idx = int(f.read().strip())
            except: idx = 0
    
    if idx >= len(ACCOUNTS): idx = len(ACCOUNTS) - 1
    email = ACCOUNTS[idx]
    remote = "gdrive" + email.replace("@gmail.com", "")
    return email, remote, idx

def switch_account(current_idx, current_email, usage_percent, albums_cache, device_name):
    next_idx = current_idx + 1
    if next_idx < len(ACCOUNTS):
        next_email = ACCOUNTS[next_idx]
        
        # 1. Alert Email
        subject = f"⚠️ Storage Full: {current_email}"
        body = (f"Account {current_email} is full ({usage_percent:.2f}%).\n"
                f"Switching to next account: {next_email}\n"
                f"Device: {device_name}")
        send_email(subject, body, device_name)
        
        # 2. Update File
        with open(ACTIVE_ACC_FILE, 'w') as f: f.write(str(next_idx))
        
        # Clear Album Cache on switch (new account = empty albums)
        albums_cache.clear()
        
        return True # Switched
    else:
        logger.error("❌ No more accounts available!")
        return False

def get_creds(email):
    token_path = os.path.join(BASE_DIR, "creds", f"token_{email}.pkl")
    if os.path.exists(token_path):
        with open(token_path, "rb") as f:
            creds = pickle.load(f)
            if creds and creds.expired and creds.refresh_token:
                try:
                    # Request refresh with the same scopes we now need
                    creds.refresh(Request())
                    with open(token_path, "wb") as f_out: 
                        pickle.dump(creds, f_out)
                except Exception as e:
                    logger.error(f"Failed to refresh token: {e}")
                    return None
            return creds
    return None
