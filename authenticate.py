# authenticate.py (using client_from_manual_flow for headless setup)
import os
from dotenv import load_dotenv
import schwab # Corrected import name
import traceback

# Load environment variables from .env file
load_dotenv() 
print("Loaded environment variables from .env file.")

# Configuration from .env
API_KEY = os.getenv('SCHWAB_APP_KEY') 
APP_SECRET = os.getenv('SCHWAB_APP_SECRET')
CALLBACK_URL = os.getenv('SCHWAB_CALLBACK_URL') 
TOKEN_PATH = os.getenv('SCHWAB_TOKEN_PATH', 'token.json') 

# --- Input Validation ---
if not all([API_KEY, APP_SECRET, CALLBACK_URL]):
    print("ERROR: Ensure SCHWAB_APP_KEY, SCHWAB_APP_SECRET, and SCHWAB_CALLBACK_URL are set in your .env file.")
    exit(1)

# Check if token already exists - if so, maybe we don't need to run this script.
if os.path.exists(TOKEN_PATH):
    print(f"\nToken file already exists at '{TOKEN_PATH}'.")
    # Optionally, add logic here to test the existing token using client_from_token_file
    # For now, we'll just inform the user and exit if the goal is only initial creation.
    # If you want this script to *always* force re-authentication, remove this check.
    print("If you need to re-authenticate, delete the existing token file and run this script again.")
    # Let's try loading it to verify
    try:
        print("Attempting to load client from existing token...")
        client = schwab.auth.client_from_token_file(TOKEN_PATH, API_KEY, APP_SECRET)
        print("Successfully loaded client from token file.")
        # Optional: Test API call
        print("Attempting to fetch account numbers...")
        response = client.get_account_numbers()
        if response.ok:
             account_numbers = response.json()
             print("Successfully fetched account numbers:")
             for acc in account_numbers:
                 print(f"  - Account Hash: {acc.get('hashValue', 'N/A')}") 
             print("Existing token is valid.")
             exit(0) # Exit successfully if token is valid
        else:
             print(f"Error fetching account numbers with existing token: {response.status_code} - {response.text}")
             print("Existing token might be expired or invalid. Consider deleting it and re-running.")
             exit(1)
    except Exception as e:
        print(f"Failed to load or test client from existing token: {e}")
        print("Proceeding to generate a new token.")
        # Optionally delete the bad token file here: os.remove(TOKEN_PATH)


# --- Proceed with Manual Flow if token doesn't exist or failed validation ---
print("\nStarting Schwab manual authentication flow...")
print("This requires you to manually open a URL and paste back the result.")
print(f" - API Key: {API_KEY[:5]}...")
print(f" - Callback URL: {CALLBACK_URL}")
print(f" - Token Path: {TOKEN_PATH}\n")

try:
    # Use client_from_manual_flow for headless environments
    client = schwab.auth.client_from_manual_flow(
        api_key=API_KEY,
        app_secret=APP_SECRET,
        callback_url=CALLBACK_URL,
        token_path=TOKEN_PATH,
        # enforce_enums=True # Keep default or set as needed
    )

    print("-" * 30)
    print(f"Authentication successful! Token data saved to {TOKEN_PATH}")

    # Optional: Test API call with the new client
        # Optional: Test API call with the new client
    print("Attempting to fetch account numbers with new token...")
    response = client.get_account_numbers()

    # --- Add these lines for debugging ---
    print(f"DEBUG: Type of response object = {type(response)}")
    print(f"DEBUG: Attributes of response object = {dir(response)}")
    # --- End of debug lines ---

    # Now, let's try checking the status code directly, 
    # which is more fundamental than .ok
    if hasattr(response, 'status_code') and response.status_code < 400:
        print(f"DEBUG: Response status code = {response.status_code}")
        try:
            account_numbers = response.json()
            print("Successfully fetched account numbers:")
            for acc in account_numbers:
                print(f"  - Account Hash: {acc.get('hashValue', 'N/A')}") 
            print("Authentication and API connection verified.")
        except Exception as json_error:
            # Handle cases where response is successful but not valid JSON
            print(f"Error parsing JSON response: {json_error}")
            raw_text = getattr(response, 'text', '<No text attribute found>')
            print(f"Raw response text: {raw_text}")

    elif hasattr(response, 'status_code'):
        # Handle API errors reported via status code
        print(f"Error fetching account numbers: Status Code {response.status_code}")
        raw_text = getattr(response, 'text', '<No text attribute found>')
        print(f"Response Text: {raw_text}")
        print("Authentication succeeded, but test API call failed.")
    else:
        # Handle cases where the response object is unexpected
        print(f"Error: Unexpected object returned by get_account_numbers: {response}")
        print("Authentication succeeded, but test API call failed.")

# Keep the original except block for broader errors
except Exception as e: 
    print(f"An error occurred during the authentication process: {e}")
    print("If you were prompted to paste a URL, ensure you pasted the FULL callback URL from your browser.")
    traceback.print_exc()

# Using a broader Exception catch first due to the previous AttributeError
# If schwab.auth has AuthenticationError in your version, you can use that.
except Exception as e: 
    print(f"An error occurred during the authentication process: {e}")
    print("If you were prompted to paste a URL, ensure you pasted the FULL callback URL from your browser.")
    traceback.print_exc()
