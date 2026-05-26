# Integration Guide: Script Reporter

This guide explains how to use the `reporter.py` module to monitor your Python scripts and receive reports via Email and/or Telegram, even if they crash.

## 1. Setup

Open the `config.yaml` file in the project root to configure your reporting channels.

### Gmail Credentials
1. Ensure your Gmail account has **2-Step Verification** enabled.
2. Generate an **App Password** (Google Account -> Security -> App passwords).
3. Configure `smtp` in `config.yaml`:
   ```yaml
   smtp:
     enabled: true
     server: "smtp.gmail.com"
     port: 587
     user: "sender@gmail.com"
     password: "app-password"
     recipient: "recipient@email.com"
   ```

### Telegram Credentials
1. Create a bot using **BotFather** on Telegram to get a **Bot Token**.
2. Start a chat with your bot or add it to a group to get the **Chat ID(s)**.
3. Configure `telegram` in `config.yaml`:
   ```yaml
   telegram:
     enabled: true
     token: "your-bot-token"
     chat_ids: 
       - "chat-id-1"
       - "chat-id-2"
   ```

## 2. Integration with Your Scripts

To send "relative data" (like number of files, size, savings), use the `state_updater` module inside your script.

### Example: Uploader Script (`uploader.py`)
```python
import time
import sys
from state_updater import updater

def main():
    print("Starting upload process...")
    
    # Initialize some data
    files = ["file1.txt", "file2.jpg", "file3.pdf"]
    uploaded_count = 0
    
    for f in files:
        print(f"Uploading {f}...")
        time.sleep(2) # Simulate work
        
        uploaded_count += 1
        
        # UPDATE THE STATE: This data survives if the script crashes!
        updater.update(
            last_file=f,
            total_uploaded=uploaded_count,
            progress=f"{uploaded_count}/{len(files)}"
        )
        
        if f == "file2.jpg":
            print("Simulating a sudden crash!")
            sys.exit(1) # Force exit to test reporting

if __name__ == "__main__":
    main()
```

## 3. Running the Reporter

Instead of running your script directly, use `reporter.py` as a wrapper:

```bash
python reporter.py uploader.py
```

### Passing Arguments
If your script takes arguments, just add them at the end:
```bash
python reporter.py my_script.py --verbose --folder ./data
```

## 4. How it Works
1. `reporter.py` starts and creates a temporary JSON file.
2. It passes the path to this file to your script via an environment variable (`REPORT_STATE_FILE`).
3. Your script's `updater.update()` writes data to this file immediately.
4. If your script crashes, `reporter.py` detects the process has ended.
5. It reads the *last saved state* from the JSON file, calculates the total duration, and sends you the email.
