import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow

# MUST match the scope in your main script
SCOPES = ['https://www.googleapis.com/auth/photoslibrary.appendonly']

def generate_token_for_account():
    print("--- Google Photos Token Generator ---")
    email = input("Enter the Gmail address you are authenticating: ").strip()
    
    creds_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'creds')
    client_secret_path = os.path.join(creds_dir, 'client_secret.json')
    if not os.path.exists(client_secret_path):
        print(f"❌ Error: client_secret.json not found in {creds_dir}!")
        return

    # Initialize the OAuth flow
    flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
    
    # This will open your browser. Log in with the specific email entered above.
    creds = flow.run_local_server(port=0)
    
    # Save the token with the naming convention expected by the main script
    token_filename = f"token_{email}.pkl"
    token_path = os.path.join(creds_dir, token_filename)
    os.makedirs(creds_dir, exist_ok=True)
    with open(token_path, 'wb') as token_file:
        pickle.dump(creds, token_file)
    
    print(f"\n✅ Success! Token saved as: {token_path}")
    print(f"Verify this file is in the creds folder to be used by your main uploader script.")

if __name__ == "__main__":
    generate_token_for_account()