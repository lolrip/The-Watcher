# monitor.py
import os
from dotenv import load_dotenv
import schwab
from schwab.client import Client
import traceback
import json
import datetime
import time
from typing import Dict, List, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("order_monitor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
load_dotenv()
logger.info("Monitor: Loaded environment variables.")

# --- Configuration ---
API_KEY = os.getenv('SCHWAB_APP_KEY')
APP_SECRET = os.getenv('SCHWAB_APP_SECRET')
CALLBACK_URL = os.getenv('SCHWAB_CALLBACK_URL')
TOKEN_PATH = os.getenv('SCHWAB_TOKEN_PATH', 'token.json')
ORDERS_CACHE_PATH = os.getenv('ORDERS_CACHE_PATH', 'active_orders.json')
CHECK_INTERVAL = float(os.getenv('CHECK_INTERVAL', '1.0'))  # Seconds between checks

# Define desired 'active' statuses
ACTIVE_STATUSES = {'WORKING', 'ACCEPTED', 'PENDING_ACTIVATION', 'QUEUED', 'AWAITING_PARENT_ORDER'}

def save_orders(orders: List[Dict[str, Any]], filepath: str = ORDERS_CACHE_PATH) -> None:
    """Save active orders to a JSON file for persistence."""
    try:
        with open(filepath, 'w') as f:
            json.dump(orders, f, indent=2)
        logger.info(f"Saved {len(orders)} active orders to {filepath}")
    except Exception as e:
        logger.error(f"Failed to save orders to {filepath}: {e}")

def load_orders(filepath: str = ORDERS_CACHE_PATH) -> List[Dict[str, Any]]:
    """Load previously saved active orders."""
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, 'r') as f:
            orders = json.load(f)
        logger.info(f"Loaded {len(orders)} orders from {filepath}")
        return orders
    except Exception as e:
        logger.error(f"Failed to load orders from {filepath}: {e}")
        return []

def fetch_active_orders(client, account_hash) -> List[Dict[str, Any]]:
    """Fetch and return active orders from the Schwab API."""
    active_orders = []
    
    orders_response = client.get_orders_for_account(account_hash=account_hash)
    
    if not orders_response.is_success:
        logger.error(f"Failed to fetch orders. Status: {orders_response.status_code}")
        logger.error(f"Response: {orders_response.text}")
        return active_orders
    
    orders_data = orders_response.json()
    
    if not isinstance(orders_data, list):
        logger.warning("Unexpected structure in orders response (expected a list)")
        return active_orders
    
    # Filter for active orders
    for order in orders_data:
        order_status = order.get('status', 'UNKNOWN').upper()
        if order_status in ACTIVE_STATUSES:
            active_orders.append(order)
    
    logger.info(f"Found {len(active_orders)} active orders out of {len(orders_data)} total orders")
    return active_orders

def extract_order_info(order_details: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and normalize key information from order details."""
    leg = order_details.get('orderLegCollection', [{}])[0]
    instrument = leg.get('instrument', {})
    
    return {
        'symbol': instrument.get('symbol', ''),
        'asset_type': instrument.get('assetType', ''),
        'instruction': leg.get('instruction'),
        'quantity': order_details.get('quantity'),
        'order_type': order_details.get('orderType'),
        'duration': order_details.get('duration'),
        'stop_price': order_details.get('stopPrice'),
        'limit_price': order_details.get('price', order_details.get('limitPrice'))
    }

def detect_asset_type(symbol: str) -> str:
    """Determine if a symbol is for an option or equity based on its format.
    
    Simply checks for numbers in the symbol - options always have numbers, equities don't.
    """
    if not symbol:
        return 'EQUITY'
        
    # If symbol contains any digits, it's an option
    return 'OPTION' if any(char.isdigit() for char in symbol) else 'EQUITY'

def place_order(client, account_hash, order_details) -> bool:
    """Place a new order based on the provided details."""
    try:
        # Extract order information
        order_info = extract_order_info(order_details)
        symbol = order_info['symbol']
        
        if not symbol:
            logger.error("Cannot place order: missing symbol")
            return False
            
        # Determine asset type using simplified detection - overrides any existing value
        # This ensures we always have a valid asset type for the API
        asset_type = detect_asset_type(symbol)
        logger.info(f"Using asset type for {symbol}: {asset_type}")
        
        # Create order request based on order type
        order_request = {
            "orderType": order_info['order_type'],
            "session": "NORMAL",
            "duration": order_info['duration'],
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": order_info['instruction'],
                    "quantity": order_info['quantity'],
                    "instrument": {
                        "symbol": symbol,
                        "assetType": asset_type
                    }
                }
            ]
        }
        
        # Add price details based on order type
        if order_info['stop_price']:
            order_request["stopPrice"] = order_info['stop_price']
        if order_info['limit_price']:
            order_request["price"] = order_info['limit_price']
            
        logger.info(f"Recreating {asset_type} order for {symbol}: {order_info['instruction']} {order_info['quantity']} {order_info['order_type']}")
        
        # Place the order
        response = client.place_order(account_hash, order_request)
        
        # Fix for the JSON parsing error - use status code as the primary success indicator
        if response.status_code in (200, 201):
            # Try to get order ID if response has JSON body
            try:
                new_order = response.json()
                order_id = new_order.get('orderId', 'unknown')
            except json.JSONDecodeError:
                # Handle empty or invalid JSON response
                order_id = "unknown - no JSON response"
                
            logger.info(f"Successfully placed new order. Order ID: {order_id}")
            return True
        else:
            logger.error(f"Failed to place order: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        traceback.print_exc()  # Include full stack trace for debugging
        return False

def monitor_orders_loop(client, account_hash, check_interval: float = CHECK_INTERVAL):
    """Main monitoring loop to track and recreate orders."""
    try:
        # Initial fetch of active orders
        current_orders = fetch_active_orders(client, account_hash)
        
        # Save initial state
        save_orders(current_orders)
        
        # Store by order ID for quick lookup
        order_by_id = {order.get('orderId'): order for order in current_orders}
        
        logger.info(f"Starting order monitoring loop. Check interval: {check_interval}s")
        logger.info(f"Initially tracking {len(order_by_id)} active orders")
        
        while True:
            time.sleep(check_interval)
            
            # Fetch current orders
            latest_orders = fetch_active_orders(client, account_hash)
            latest_order_ids = {order.get('orderId') for order in latest_orders}
            
            # Check for disappeared orders
            for order_id, order_data in list(order_by_id.items()):
                if order_id not in latest_order_ids:
                    logger.warning(f"Order {order_id} for {order_data.get('orderLegCollection', [{}])[0].get('instrument', {}).get('symbol')} is no longer active")
                    
                    # Attempt to recreate the order
                    if place_order(client, account_hash, order_data):
                        logger.info(f"Successfully recreated order for {order_data.get('orderLegCollection', [{}])[0].get('instrument', {}).get('symbol')}")
            
            # Update our tracking with the latest orders
            order_by_id = {order.get('orderId'): order for order in latest_orders}
            save_orders(latest_orders)
            
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")
    except Exception as e:
        logger.error(f"Error in monitoring loop: {e}")
        traceback.print_exc()

# Main execution flow
def main():
    try:
        # Validate environment variables
        if not all([API_KEY, APP_SECRET, CALLBACK_URL, TOKEN_PATH]):
            logger.error("Ensure SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_CALLBACK_URL, and SCHWAB_TOKEN_PATH are set.")
            exit(1)

        if not os.path.exists(TOKEN_PATH):
            logger.error(f"Token file not found at '{TOKEN_PATH}'. Please run authenticate.py first.")
            exit(1)

        logger.info("Initializing Schwab client...")
        client = schwab.auth.easy_client(
            api_key=API_KEY,
            app_secret=APP_SECRET,
            callback_url=CALLBACK_URL,
            token_path=TOKEN_PATH,
            interactive=False
        )
        logger.info("Schwab client initialized successfully.")

        # Get account hash
        acc_num_response = client.get_account_numbers()
        
        if not acc_num_response.is_success:
            logger.error(f"Failed to fetch account numbers: {acc_num_response.status_code}")
            exit(1)
            
        account_numbers_data = acc_num_response.json()
        if not account_numbers_data:
            logger.error("No accounts found.")
            exit(1)
            
        account_hash = account_numbers_data[0].get('hashValue')
        if not account_hash:
            logger.error("Could not extract account hash from account data")
            exit(1)
            
        logger.info(f"Using Account Hash: {account_hash[:10]}...")
        
        # Start monitoring loop
        monitor_orders_loop(client, account_hash)
            
    except (schwab.exceptions.AuthenticationError, schwab.exceptions.AccessTokenError) as auth_err:
        logger.error(f"Authentication failed: {auth_err}")
        logger.error("The refresh token might be expired (approx 7 days) or the token file is invalid.")
        logger.error("Try running authenticate.py again.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()