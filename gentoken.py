import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow

# MUST match the scope in your main script
SCOPES = ['https://www.googleapis.com/auth/photoslibrary.appendonly']

def generate_token_for_account():
    print("--- Google Photos Token Generator ---")
    email = input("Enter the Gmail address you are authenticating: ").strip()
    
    if not os.path.exists('client_secret.json'):
        print("❌ Error: client_secret.json not found in this folder!")
        return

    # Initialize the OAuth flow
    flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
    
    # This will open your browser. Log in with the specific email entered above.
    creds = flow.run_local_server(port=0)
    
    # Save the token with the naming convention expected by the main script
    token_filename = f"token_{email}.pkl"
    with open(token_filename, 'wb') as token_file:
        pickle.dump(creds, token_file)
    
    print(f"\n✅ Success! Token saved as: {token_filename}")
    print(f"Verify this file is in the same folder as your main uploader script.")

if __name__ == "__main__":
    generate_token_for_account()