import os
import smtplib
import yaml
import requests
import json
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
    """
    config = load_config()
    if not config:
        return

    # 1. SMTP
    smtp_cfg = config.get('smtp', {})
    if smtp_cfg.get('enabled', False):
        _send_email(subject, body, smtp_cfg, is_html)

    # 2. Telegram
    tg_cfg = config.get('telegram', {})
    if tg_cfg.get('enabled', False):
        _send_telegram(f"<b>{subject}</b>\n\n{body}", tg_cfg)

def _send_email(subject, body, smtp_cfg, is_html):
    required_keys = ['server', 'port', 'user', 'password', 'recipient']
    if not all(key in smtp_cfg for key in required_keys):
        return

    msg = MIMEMultipart()
    msg['From'] = smtp_cfg['user']
    msg['To'] = smtp_cfg['recipient']
    msg['Subject'] = subject

    content_type = 'html' if is_html else 'plain'
    msg.attach(MIMEText(body, content_type))

    try:
        server = smtplib.SMTP(smtp_cfg['server'], smtp_cfg['port'])
        server.starttls()
        server.login(smtp_cfg['user'], smtp_cfg['password'])
        server.send_message(msg)
        server.quit()
    except Exception:
        pass

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
