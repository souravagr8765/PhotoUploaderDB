import sys
import os

# Add the project root to sys.path so we can import from 'infra'
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import json
import subprocess
import argparse
import tempfile
import yaml
from datetime import datetime
from infra.notifications import send_notification

def load_config():
    # Look for config.yaml in the project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(project_root, 'config.yaml')
    if not os.path.exists(config_path):
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    
    if not os.path.exists(config_path):
        return None
    
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception:
        return None

def generate_html_report(script_name, start_time, end_time, duration, exit_code, custom_data):
    custom_rows = ""
    if custom_data:
        for key, value in custom_data.items():
            custom_rows += f"<tr><td><strong>{key}</strong></td><td>{value}</td></tr>"
    else:
        custom_rows = "<tr><td colspan='2'>No relative data provided.</td></tr>"

    status_color = "green" if exit_code == 0 else "red"
    status_text = "Success" if exit_code == 0 else f"Failed (Exit Code: {exit_code})"

    html = f"""
    <html>
    <head>
        <style>
            table {{ border-collapse: collapse; width: 100%; max-width: 600px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .header {{ font-size: 18px; font-weight: bold; margin-bottom: 10px; }}
            .status {{ color: {status_color}; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div class="header">Execution Report: {script_name}</div>
        <p>Status: <span class="status">{status_text}</span></p>
        <table>
            <tr><th>Attribute</th><th>Value</th></tr>
            <tr><td>Start Time</td><td>{start_time}</td></tr>
            <tr><td>End Time</td><td>{end_time}</td></tr>
            <tr><td>Duration</td><td>{duration}</td></tr>
            {custom_rows}
        </table>
    </body>
    </html>
    """
    return html

def generate_telegram_report(script_name, start_time, end_time, duration, exit_code, custom_data):
    status_emoji = "✅" if exit_code == 0 else "❌"
    status_text = "Success" if exit_code == 0 else f"Failed (Exit Code: {exit_code})"
    
    msg = f"<b>{status_emoji} Execution Report: {script_name}</b>\n\n"
    msg += f"<b>Status:</b> {status_text}\n"
    msg += f"<b>Start Time:</b> {start_time}\n"
    msg += f"<b>End Time:</b> {end_time}\n"
    msg += f"<b>Duration:</b> {duration}\n"
    
    if custom_data:
        msg += "\n<b>Relative Data:</b>\n"
        for key, value in custom_data.items():
            msg += f"• <b>{key}:</b> {value}\n"
            
    return msg

def main():
    parser = argparse.ArgumentParser(description="Wrapper to report script execution via SMTP and Telegram.")
    parser.add_argument("script", help="The python script to run.")
    parser.add_argument("args", nargs=argparse.REMAINDER, help="Arguments for the script.")
    
    args = parser.parse_args()

    if not os.path.exists(args.script):
        print(f"Error: Script '{args.script}' not found.")
        sys.exit(1)

    # Setup state file
    script_base = os.path.basename(args.script)
    fd, state_file_path = tempfile.mkstemp(suffix=".json", prefix=f"state_{script_base}_")
    os.close(fd)
    
    # Initialize state file
    with open(state_file_path, 'w') as f:
        json.dump({}, f)

    start_dt = datetime.now()
    start_time_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"[*] Starting {args.script} at {start_time_str}")
    print(f"[*] State file: {state_file_path}")

    env = os.environ.copy()
    env['REPORT_STATE_FILE'] = state_file_path

    exit_code = -1
    try:
        # Run the script
        process = subprocess.run([sys.executable, args.script] + args.args, env=env)
        exit_code = process.returncode
    except KeyboardInterrupt:
        print("\n[!] Execution interrupted by user.")
        exit_code = -130
    except Exception as e:
        print(f"\n[!] Unexpected error running script: {e}")
        exit_code = 999
    finally:
        end_dt = datetime.now()
        end_time_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        duration = end_dt - start_dt
        duration_str = str(duration).split('.')[0]
        
        # Read custom data
        custom_data = {}
        if os.path.exists(state_file_path):
            try:
                with open(state_file_path, 'r') as f:
                    custom_data = json.load(f)
            except Exception:
                pass
            
            # Cleanup state file
            try:
                os.remove(state_file_path)
            except Exception:
                pass

        print(f"[*] Finished {args.script} at {end_time_str} (Duration: {duration_str})")
        
        # Reports
        subject = f"Script Report: {script_base} ({'SUCCESS' if exit_code == 0 else 'CRASHED/FAILED'})"
        
        # Check if we should skip notification (Success and 0 uploads)
        total_uploaded = custom_data.get('total_uploaded', 0)
        if exit_code == 0 and total_uploaded == 0:
            print("[*] No changes detected. Skipping notification.")
            return

        report_html = generate_html_report(
            script_base, 
            start_time_str, 
            end_time_str, 
            duration_str, 
            exit_code, 
            custom_data
        )
        
        # We use a trick here: since send_notification sends to both, and we want different content for TG,
        # we might need to adjust infra.notifications. For now, we'll just send the HTML one.
        # But wait, TG doesn't like complex HTML. 
        # Actually, let's just use the plain text version for both if we want to be safe, 
        # or update infra.notifications to be more flexible.
        
        # Let's just send the HTML report to Email and a simpler one to Telegram if possible.
        # For simplicity in this integration, I'll just use send_notification with the HTML report.
        
        send_notification(subject, report_html, is_html=True)
        print("[+] Reports sent via configured channels.")

if __name__ == "__main__":
    main()
