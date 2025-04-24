import threading
import time
import datetime
import monitor
import os
from dotenv import load_dotenv
import logging
import schwab
import json
from flask import Flask, render_template, jsonify, request, Response # Ensure Response is imported
from functools import wraps # Import wraps for decorator

# --- Existing Setup ---
logging.basicConfig(
    level=logging.INFO, # Consider DEBUG for more detailed lock info
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s' # Added threadName
)
logger = logging.getLogger(__name__)
load_dotenv()
app = Flask(__name__)

# --- Basic Auth Configuration ---
FLASK_USERNAME = os.getenv('FLASK_USERNAME')
FLASK_PASSWORD = os.getenv('FLASK_PASSWORD')

# --- Basic Auth Helper Functions ---
def check_auth(username, password):
    """Check if a username/password combination is valid."""
    return username == FLASK_USERNAME and password == FLASK_PASSWORD

def authenticate():
    """Sends a 401 response that enables basic auth."""
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # Skip auth if no credentials are set in .env (allows local dev without auth)
        if not FLASK_USERNAME or not FLASK_PASSWORD:
            return f(*args, **kwargs)

        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

# --- State and Lock ---
current_state = {
    "active_orders": [],
    "monitoring_active": False,
    "last_updated": None,
    "orders_recreated": 0,
    "positions": {"long": 0, "short": 0},
    "net_liq_history": [],
    "ignored_orders": set() # Ensure initialized
}
state_lock = threading.Lock()
ignored_symbols = set() # Assuming this is still global for now

# --- Utility Functions (Modified) ---

def prepare_for_json(obj):
    """Prepare an object for JSON serialization by converting non-serializable types."""
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, (datetime.datetime, datetime.date)):
         return obj.isoformat() # Handle datetime objects if they appear
    elif isinstance(obj, dict):
        return {k: prepare_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [prepare_for_json(i) for i in obj]
    # Add handling for other non-serializable types if necessary
    return obj

def get_order_symbol(order):
    """Extract symbol from order data safely"""
    try:
        # Check primary instrument first (common for single-leg orders)
        instrument = order.get('instrument')
        if instrument and isinstance(instrument, dict):
             symbol = instrument.get('symbol')
             if symbol: return symbol

        # Fallback to orderLegCollection
        legs = order.get('orderLegCollection', [])
        if legs and isinstance(legs, list) and len(legs) > 0:
            leg_instrument = legs[0].get('instrument')
            if leg_instrument and isinstance(leg_instrument, dict):
                return leg_instrument.get('symbol')
    except Exception as e:
        logger.warning(f"Could not extract symbol from order: {order.get('orderId', 'N/A')}. Error: {e}", exc_info=True)
    return None

def is_order_monitored(order_id, symbol, ignored_orders_set, ignored_symbols_set):
    """Check if an order should be monitored based on ID and symbol ignore lists."""
    if order_id is None:
        return True # Cannot ignore without an ID

    order_id_str = str(order_id)

    if order_id_str in ignored_orders_set:
        # logger.debug(f"Order ID {order_id_str} is explicitly ignored.")
        return False
    # if symbol and symbol in ignored_symbols_set: # Uncomment if symbol ignoring is needed here
    #     logger.debug(f"Symbol {symbol} (Order ID {order_id_str}) is ignored.")
    #     return False

    # logger.debug(f"Order ID {order_id_str} (Symbol: {symbol}) is monitored.")
    return True

def update_monitoring_status_in_orders(orders_list, ignored_orders_set):
    """Update isMonitored flag in all orders based on current ignore lists.
       Assumes this is called when the state_lock might be held or immediately after.
    """
    if not isinstance(orders_list, list):
        logger.warning("update_monitoring_status_in_orders received non-list for orders.")
        return

    for order in orders_list:
         if not isinstance(order, dict): continue # Skip non-dict items
         order_id = order.get("orderId")
         symbol = get_order_symbol(order)
         # Update the flag based on the provided ignored_orders_set
         order["isMonitored"] = is_order_monitored(
             order_id, symbol, ignored_orders_set, ignored_symbols # Pass current sets
         )

def save_ignored_items():
    """Save ignored orders and symbols to disk, minimizing lock time."""
    ignored_orders_list = []
    ignored_symbols_list = []
    try:
        # --- Acquire lock only to read data ---
        with state_lock:
            # logger.debug("Acquired lock for save_ignored_items read")
            # Safely copy the sets to lists for saving
            ignored_orders_list = list(current_state.get('ignored_orders', set()))
            ignored_symbols_list = list(ignored_symbols) # Assuming global or move to current_state
            # logger.debug("Releasing lock for save_ignored_items read")
        # --- Lock released ---

        # --- Perform file I/O outside the lock ---
        logger.debug(f"Performing file I/O for save_ignored_items. Orders: {len(ignored_orders_list)}, Symbols: {len(ignored_symbols_list)}")
        with open('ignored_items.json', 'w') as f:
            json.dump({
                'orders': ignored_orders_list,
                'symbols': ignored_symbols_list
            }, f, indent=2) # Add indent for readability
        logger.info(f"Saved ignored items to disk (Orders: {len(ignored_orders_list)}, Symbols: {len(ignored_symbols_list)})")
        # logger.debug("Finished file I/O for save_ignored_items")

    except Exception as e:
        logger.error(f"Failed to save ignored items: {e}", exc_info=True) # Add traceback

def load_ignored_items():
    """Load previously saved ignored orders and symbols from disk."""
    global ignored_symbols
    try:
        if not os.path.exists('ignored_items.json'):
            logger.info("No ignored items file found. Starting with empty ignore lists.")
            # Ensure sets exist even if file doesn't
            with state_lock:
                current_state["ignored_orders"] = set()
            ignored_symbols = set()
            return

        with open('ignored_items.json', 'r') as f:
            data = json.load(f)

        loaded_ignored_orders = set()
        loaded_ignored_symbols = set()

        if 'orders' in data and isinstance(data['orders'], list):
            loaded_ignored_orders = set(str(id) for id in data['orders'])

        if 'symbols' in data and isinstance(data['symbols'], list):
            loaded_ignored_symbols = set(str(symbol) for symbol in data['symbols'])

        # Update global/shared state under lock
        with state_lock:
            # logger.debug("Acquired lock for load_ignored_items write")
            current_state["ignored_orders"] = loaded_ignored_orders
            ignored_symbols = loaded_ignored_symbols # Update global
            # logger.debug("Released lock for load_ignored_items write")

        logger.info(f"Loaded {len(loaded_ignored_orders)} ignored orders and {len(loaded_ignored_symbols)} ignored symbols")

    except FileNotFoundError:
         logger.info("ignored_items.json not found, starting fresh.")
         with state_lock:
             current_state["ignored_orders"] = set()
         ignored_symbols = set()
    except Exception as e:
        logger.error(f"Failed to load ignored items: {e}", exc_info=True)
        # Ensure sets exist even on error
        with state_lock:
            if "ignored_orders" not in current_state:
                current_state["ignored_orders"] = set()
        if 'ignored_symbols' not in globals(): # Or check if it's None
             ignored_symbols = set()


# --- Flask Routes (Modified) ---

@app.route('/')
@requires_auth # Apply the authentication decorator
def index():
    return render_template('index.html')

@app.route('/api/orders')
@requires_auth # Apply the authentication decorator
def get_orders():
    with state_lock:
        # logger.debug("Acquired lock for get_orders")
        # Create a deep copy to avoid modifying state while serializing
        state_copy = current_state.copy()
        # Ensure ignored_orders is included if it exists
        if "ignored_orders" in state_copy:
             state_copy["ignored_orders"] = list(state_copy["ignored_orders"]) # Convert set for JSON

        # Update isMonitored flag before sending
        update_monitoring_status_in_orders(
            state_copy.get("active_orders", []),
            current_state.get("ignored_orders", set()) # Use the live set for checking
        )
        # logger.debug("Releasing lock for get_orders")

    # Serialize outside the lock
    try:
        serializable_state = prepare_for_json(state_copy)
        return jsonify(serializable_state)
    except Exception as e:
        logger.error(f"Error serializing state for /api/orders: {e}", exc_info=True)
        return jsonify({"error": "Failed to serialize state"}), 500


@app.route('/api/orders/<order_id>/stop_monitoring', methods=['POST'])
@requires_auth # Apply the authentication decorator
def stop_order_monitoring(order_id):
    """Permanently stop monitoring for a specific order (non-blocking)."""
    order_id_str = str(order_id) # Ensure string comparison
    logger.info(f"Received request to stop monitoring order {order_id_str}")
    needs_save = False
    try:
        # --- Update in-memory state quickly ---
        with state_lock:
            logger.debug(f"Acquired lock for stop_monitoring {order_id_str}")
            # Ensure the set exists
            if "ignored_orders" not in current_state:
                current_state["ignored_orders"] = set()

            if order_id_str not in current_state["ignored_orders"]:
                current_state["ignored_orders"].add(order_id_str)
                needs_save = True
                logger.info(f"Added order {order_id_str} to ignore list (in memory).")

                # Update monitoring status in the current active orders list immediately
                update_monitoring_status_in_orders(
                    current_state.get("active_orders", []),
                    current_state["ignored_orders"] # Pass the updated set
                )
            else:
                logger.info(f"Order {order_id_str} was already in the ignore list.")
            logger.debug(f"Released lock for stop_monitoring {order_id_str}")
        # --- Lock released ---

        # --- Perform file I/O outside the lock IF needed ---
        if needs_save:
            logger.debug(f"Initiating save ignored items after stopping {order_id_str}")
            save_ignored_items() # This function now handles its own locking minimally
            logger.debug(f"Completed save ignored items after stopping {order_id_str}")
        else:
             logger.debug(f"No save needed for {order_id_str}")

        return jsonify({
            "success": True,
            "order_id": order_id_str,
            "message": f"Monitoring stopped for order {order_id_str}"
        })

    except Exception as e:
        logger.error(f"Error in stop_order_monitoring for {order_id_str}: {e}", exc_info=True)
        # Avoid saving if the error occurred before file I/O decision
        return jsonify({"success": False, "message": f"Error stopping monitoring: {e}"}), 500


@app.route('/api/orders/<order_id>/toggle_monitoring', methods=['POST'])
@requires_auth # Apply the authentication decorator
def toggle_order_monitoring(order_id):
    """Enable/disable monitoring for a specific order (non-blocking)."""
    order_id_str = str(order_id)
    logger.info(f"Received request to toggle monitoring for order {order_id_str}")
    needs_save = False
    current_monitoring_status = False # Default assumption
    try:
        data = request.json
        if not data:
             return jsonify({"success": False, "message": "Missing request body"}), 400
        should_monitor = data.get('monitor', False) # True to monitor, False to ignore

        # --- Update in-memory state quickly ---
        with state_lock:
            logger.debug(f"Acquired lock for toggle_monitoring {order_id_str}")
            if "ignored_orders" not in current_state:
                current_state["ignored_orders"] = set()

            currently_ignored = order_id_str in current_state["ignored_orders"]
            current_monitoring_status = not currently_ignored

            if should_monitor and currently_ignored:
                # Resume monitoring: remove from ignored list
                current_state["ignored_orders"].discard(order_id_str)
                needs_save = True
                logger.info(f"Resumed monitoring for order {order_id_str} (in memory).")
            elif not should_monitor and not currently_ignored:
                # Stop monitoring: add to ignored list
                current_state["ignored_orders"].add(order_id_str)
                needs_save = True
                logger.info(f"Stopped monitoring for order {order_id_str} (in memory).")
            else:
                logger.info(f"Order {order_id_str} monitoring state already as requested ({should_monitor}). No change.")

            # Update monitoring status in the current active orders list if state changed
            if needs_save:
                 update_monitoring_status_in_orders(
                     current_state.get("active_orders", []),
                     current_state["ignored_orders"] # Pass the updated set
                 )

            # Read the final status after potential change
            current_monitoring_status = order_id_str not in current_state["ignored_orders"]
            logger.debug(f"Released lock for toggle_monitoring {order_id_str}")
        # --- Lock released ---

        # --- Perform file I/O outside the lock IF needed ---
        if needs_save:
            logger.debug(f"Initiating save ignored items after toggling {order_id_str}")
            save_ignored_items()
            logger.debug(f"Completed save ignored items after toggling {order_id_str}")
        else:
             logger.debug(f"No save needed for {order_id_str}")

        return jsonify({
            "success": True,
            "monitoring": current_monitoring_status, # Return the actual final state
            "order_id": order_id_str
        })

    except Exception as e:
        logger.error(f"Error in toggle_order_monitoring for {order_id_str}: {e}", exc_info=True)
        return jsonify({"success": False, "message": f"Error toggling monitoring: {e}"}), 500


@app.route('/api/token-status')
@requires_auth # Apply the authentication decorator
def get_token_status():
    """Return token expiration status for the UI"""
    try:
        # Read the token file directly to avoid authenticating again
        token_path = os.getenv('SCHWAB_TOKEN_PATH', 'token.json')
        if not os.path.exists(token_path):
            # Return a structured response when token file not found
            return jsonify({
                "error": "Token file not found",
                "expires_in": 0,
                "expires_at": 0,
                "current_time": time.time(),
                "creation_time": 0,
                "refresh_token_age": 0
            }), 404

        with open(token_path, 'r') as f:
            token_data = json.load(f)

        # Calculate expiration time for access token
        current_time = time.time()
        # Navigate the structure safely with defaults
        expires_at = token_data.get('token', {}).get('expires_at', 0)

        # Calculate seconds until expiration for access token
        expires_in = max(0, expires_at - current_time)

        # Calculate refresh token age (refresh tokens usually valid for 7 days)
        creation_timestamp = token_data.get('creation_timestamp', current_time)
        refresh_token_age = (current_time - creation_timestamp) / 86400  # Convert to days

        return jsonify({
            "expires_in": expires_in,
            "expires_at": expires_at,
            "current_time": current_time,
            "creation_time": creation_timestamp,
            "refresh_token_age": refresh_token_age,
            "refresh_token_age_days": round(refresh_token_age, 1)
        })
    except Exception as e:
        logger.error(f"Error getting token status: {e}", exc_info=True)
        return jsonify({
            "error": str(e),
            "expires_in": 0,
            "expires_at": 0,
            "current_time": time.time(),
            "creation_time": 0,
            "refresh_token_age": 0
        }), 500


# --- Background Monitor (Minor adjustments for clarity) ---

def background_monitor():
    """Background thread to monitor orders and update dashboard state"""
    global current_state # Ensure we're modifying the global state

    try:
        logger.info("Initializing Schwab client for background monitor...")
        client = get_schwab_client() # Assumes this handles auth correctly
        account_hash = get_account_hash(client)
        logger.info(f"Starting background monitoring for account {account_hash[:8]}...")

        orders_recreated_count = 0
        tracked_orders = {} # {order_id_str: order_data} - Use strings for consistency

        # Initialize monitoring state
        with state_lock:
            # logger.debug("Acquired lock for background_monitor init")
            current_state["monitoring_active"] = True
            if "ignored_orders" not in current_state:
                current_state["ignored_orders"] = set()
            # logger.debug("Released lock for background_monitor init")

        while True:
            # logger.debug("Background monitor loop start.")
            local_ignored_orders = set()
            local_ignored_symbols = set() # If used

            try:
                # --- Fetch data (API calls outside lock) ---
                # logger.debug("Fetching active orders...")
                active_orders_list = monitor.fetch_active_orders(client, account_hash) # Assuming this returns a list
                # logger.debug(f"Fetched {len(active_orders_list)} orders.")

                if not isinstance(active_orders_list, list):
                     logger.error(f"fetch_active_orders returned type {type(active_orders_list)}, expected list. Skipping cycle.")
                     time.sleep(10) # Wait before retrying
                     continue

                latest_order_ids = {str(order.get('orderId')) for order in active_orders_list if order.get('orderId')}
                # logger.debug(f"Latest active order IDs: {latest_order_ids}")

                # --- Get current ignore lists (brief lock) ---
                with state_lock:
                    # logger.debug("Acquired lock for reading ignore lists")
                    local_ignored_orders = current_state.get("ignored_orders", set()).copy()
                    local_ignored_symbols = ignored_symbols.copy() # Assuming global
                    # logger.debug("Released lock for reading ignore lists")

                # --- Process orders (logic outside lock, using local copies of ignore lists) ---
                current_tracked_ids = set(tracked_orders.keys())
                disappeared_order_ids = current_tracked_ids - latest_order_ids
                new_order_ids = latest_order_ids - current_tracked_ids

                orders_to_recreate = []

                # Process disappeared orders
                for order_id_str in disappeared_order_ids:
                    order_data = tracked_orders.pop(order_id_str, None) # Remove from tracking
                    if order_data:
                        symbol = get_order_symbol(order_data)
                        # Check if it *should* be monitored before deciding to recreate
                        if is_order_monitored(order_id_str, symbol, local_ignored_orders, local_ignored_symbols):
                            logger.warning(f"Monitored order {order_id_str} ({symbol}) disappeared. Queuing for recreation.")
                            orders_to_recreate.append(order_data)
                        else:
                            logger.info(f"Ignored order {order_id_str} ({symbol}) disappeared. No action needed.")
                    else:
                         logger.warning(f"Order ID {order_id_str} disappeared but wasn't in tracked_orders.")


                # Update tracked orders with latest data / add new orders
                for order in active_orders_list:
                     order_id = order.get('orderId')
                     if order_id:
                         order_id_str = str(order_id)
                         tracked_orders[order_id_str] = order # Update or add

                # Remove any orders from tracking that are now ignored but still active
                # (This handles cases where an order is ignored *while* it's active)
                for order_id_str in list(tracked_orders.keys()):
                     symbol = get_order_symbol(tracked_orders[order_id_str])
                     if not is_order_monitored(order_id_str, symbol, local_ignored_orders, local_ignored_symbols):
                         logger.info(f"Order {order_id_str} ({symbol}) is now ignored. Removing from active tracking for recreation.")
                         tracked_orders.pop(order_id_str, None)


                # --- Update shared state (acquire lock) ---
                positions_data, net_liq = get_account_data(client, account_hash)
                timestamp = datetime.datetime.now().isoformat()
                positions = analyze_combined_data(active_orders_list, positions_data)

                with state_lock:
                    # logger.debug("Acquired lock for state update")
                    # Update the list of orders shown in the UI
                    current_state["active_orders"] = active_orders_list
                    # Update the isMonitored flag based on the latest ignore list
                    update_monitoring_status_in_orders(
                        current_state["active_orders"],
                        local_ignored_orders # Use the set we checked against
                    )

                    current_state["last_updated"] = timestamp
                    current_state["positions"] = positions
                    current_state["orders_recreated"] = orders_recreated_count # Update count

                    # Update net liquidation history
                    current_state["net_liq_history"].append((timestamp, net_liq))
                    if len(current_state["net_liq_history"]) > 100: # Limit history size
                        current_state["net_liq_history"] = current_state["net_liq_history"][-100:]
                    # logger.debug("Released lock for state update")
                # --- Lock released ---


                # --- Recreate disappeared orders (API calls outside lock) ---
                for order_data in orders_to_recreate:
                    symbol = get_order_symbol(order_data)
                    logger.info(f"Attempting to recreate order for {symbol} (ID: {order_data.get('orderId')})")
                    # Ensure place_order uses the correct assetType logic from monitor.py 3
                    if monitor.place_order(client, account_hash, order_data):
                        orders_recreated_count += 1
                        # Update count in shared state immediately after success
                        with state_lock:
                             current_state["orders_recreated"] = orders_recreated_count
                        logger.info(f"Successfully recreated order for {symbol}. Total recreated: {orders_recreated_count}")
                    else:
                        logger.error(f"Failed to recreate order for {symbol} (ID: {order_data.get('orderId')}). Will retry next cycle if it remains disappeared.")
                        # Add the order back to tracked_orders so we try again next time?
                        # Or rely on it disappearing again in the next fetch?
                        # Let's rely on the next fetch: if recreation fails, it won't be in latest_order_ids
                        # and should be picked up again in the next cycle's 'disappeared' check.


                # logger.info(f"Monitor cycle complete. Tracked orders: {len(tracked_orders)}. Ignored: {len(local_ignored_orders)}")
                time.sleep(1) # Check interval

            except schwab.exceptions.AccessTokenError as e:
                 logger.error(f"Schwab Access Token Error in monitor loop: {e}. Attempting to refresh/re-auth might be needed.", exc_info=True)
                 # Potentially add logic here to force a token refresh or alert the user
                 time.sleep(60) # Wait longer after auth errors
            except schwab.exceptions.GeneralError as e:
                 logger.error(f"Schwab General API Error in monitor loop: {e}", exc_info=True)
                 time.sleep(15) # Wait a bit longer after general API errors
            except ConnectionError as e:
                 logger.error(f"Network Connection Error in monitor loop: {e}", exc_info=True)
                 time.sleep(30) # Wait longer if network is down
            except Exception as e:
                logger.error(f"Unexpected error in monitoring loop: {e}", exc_info=True)
                time.sleep(10) # Back off on unexpected error

    except Exception as e:
        logger.critical(f"Background monitoring thread failed critically: {e}", exc_info=True)
        with state_lock:
            current_state["monitoring_active"] = False

# --- Other Helper Functions (get_schwab_client, get_account_hash, etc. - assumed ok) ---
# Make sure get_schwab_client uses easy_client which handles token refresh 1
def get_schwab_client():
    """Create and return authenticated Schwab client"""
    API_KEY = os.getenv('SCHWAB_APP_KEY')
    APP_SECRET = os.getenv('SCHWAB_APP_SECRET')
    CALLBACK_URL = os.getenv('SCHWAB_CALLBACK_URL')
    TOKEN_PATH = os.getenv('SCHWAB_TOKEN_PATH', 'token.json')

    if not all([API_KEY, APP_SECRET, CALLBACK_URL, TOKEN_PATH]):
        logger.critical("Missing Schwab API credentials in environment variables!")
        raise ValueError("Schwab API credentials not configured.")

    try:
        # easy_client handles token loading, validation, and refreshing
        client = schwab.auth.easy_client(
            api_key=API_KEY,
            app_secret=APP_SECRET,
            callback_url=CALLBACK_URL,
            token_path=TOKEN_PATH,
            interactive=False # Important for background operation
        )
        logger.info("Schwab client initialized via easy_client.")
        # Perform a quick test call to ensure token validity early
        test_resp = client.get_account_numbers()
        if not test_resp.is_success:
             logger.warning(f"Initial client test failed: {test_resp.status_code} - {test_resp.text}. Token might be expired.")
             # easy_client should ideally handle refresh, but log warning.
        else:
             logger.info("Schwab client test call successful.")
        return client
    except Exception as e:
        logger.critical(f"Failed to initialize Schwab client: {e}", exc_info=True)
        raise # Re-raise critical error


def get_account_hash(client):
    """Get the first account hash from the client"""
    try:
        acc_num_response = client.get_account_numbers()
        if not acc_num_response.is_success:
             logger.error(f"Failed to get account numbers: {acc_num_response.status_code} - {acc_num_response.text}")
             raise ConnectionError("Failed to retrieve account numbers from Schwab.") # Or a more specific error

        account_numbers_data = acc_num_response.json()
        if not account_numbers_data or not isinstance(account_numbers_data, list) or len(account_numbers_data) == 0:
            logger.error("No account numbers found in Schwab response.")
            raise ValueError("No Schwab accounts found.")

        # Find the hashValue, handling potential missing keys
        first_account = account_numbers_data[0]
        if not isinstance(first_account, dict):
             logger.error(f"Unexpected account data format: {first_account}")
             raise ValueError("Unexpected Schwab account data format.")

        account_hash = first_account.get('hashValue')
        if not account_hash:
            logger.error(f"Could not find 'hashValue' in account data: {first_account}")
            raise ValueError("Could not extract account hash from Schwab response.")

        return account_hash
    except Exception as e:
        logger.error(f"Error getting account hash: {e}", exc_info=True)
        raise # Re-raise after logging


def get_account_data(client, account_hash):
    """Fetch account data including positions and balances"""
    try:
        # Request positions data from the account endpoint
        response = client.get_account(account_hash, fields=client.Account.Fields.POSITIONS)
        if not response.is_success:
            logger.warning(f"Failed to fetch account details: {response.status_code} - {response.text}")
            return None, 0  # Return None for positions and 0 for net_liq
            
        account_data = response.json()
        
        # Navigate the structure carefully
        securities_account = account_data.get('securitiesAccount', {})
        if not securities_account:
            logger.warning("Missing securitiesAccount in response")
            return None, 0
            
        # Extract positions
        positions = securities_account.get('positions', [])
        
        # Extract liquidation value
        current_balances = securities_account.get('currentBalances', {})
        net_liq = current_balances.get('liquidationValue', 0)
        
        return positions, net_liq
    except Exception as e:
        logger.error(f"Error fetching account data: {e}", exc_info=True)
        return None, 0


def analyze_combined_data(orders_list, positions_data):
    """
    Analyze both orders and positions & enrich orders with current price data.
    Counts total SPX contracts (not just positions) for accurate reporting.
    """
    position_stats = {
        "long_count": 0,           # Number of positions with long_qty > 0
        "short_count": 0,          # Number of positions with short_qty > 0
        "spx_long_contracts": 0,   # Total SPX long contracts
        "spx_short_contracts": 0,  # Total SPX short contracts
        "spx_active_stops": 0,
        "spx_closing": 0
    }
    
    symbols_with_stops = set()
    symbols_with_closing = set()
    
    current_prices = {}
    MULTIPLIER = 100

    # Helper to determine if a symbol is an SPX option
    def is_spx_option(symbol):
        return symbol and 'SPX' in symbol.upper() and any(x in symbol.upper() for x in ['C', 'P'])

    if positions_data and isinstance(positions_data, list):
        spx_positions_debug = []
        for position in positions_data:
            if not isinstance(position, dict):
                continue
            try:
                instrument = position.get('instrument', {})
                symbol = instrument.get('symbol', '')
                if not symbol:
                    continue

                market_value = float(position.get('marketValue', 0))
                long_qty = float(position.get('longQuantity', 0))
                short_qty = float(position.get('shortQuantity', 0))

                is_spx = symbol and 'SPX' in symbol.upper()
                if is_spx:
                    # Add position to debug list
                    spx_positions_debug.append({
                        "symbol": symbol,
                        "long_qty": long_qty,
                        "short_qty": short_qty,
                        "market_value": market_value
                    })
                    # Sum total contracts for SPX
                    position_stats["spx_long_contracts"] += long_qty
                    position_stats["spx_short_contracts"] += short_qty

                if long_qty > 0 and market_value != 0:
                    # Calculate price
                    computed_price = market_value / long_qty
                    
                    # For SPX options: ALWAYS divide by multiplier (not conditional)
                    if is_spx_option(symbol):
                        computed_price /= MULTIPLIER
                        logger.debug(f"Adjusted {symbol} price: {computed_price}")
                    current_prices[symbol] = computed_price
                elif short_qty > 0 and market_value != 0:
                    computed_price = abs(market_value / short_qty)
                    
                    # For SPX options: ALWAYS divide by multiplier (not conditional)
                    if is_spx_option(symbol):
                        computed_price /= MULTIPLIER
                        logger.debug(f"Adjusted {symbol} price: {computed_price}")
                    current_prices[symbol] = computed_price

                if long_qty > 0:
                    position_stats["long_count"] += 1
                if short_qty > 0:
                    position_stats["short_count"] += 1
            except Exception as e:
                logger.warning(f"Error processing position for {symbol}: {e}")

        # Log summary for SPX
        logger.info("=== SPX Position Debug Summary ===")
        for pos in spx_positions_debug:
            logger.info(
                f"SPX {pos['symbol']}: long_qty={pos['long_qty']}, short_qty={pos['short_qty']}, market_value={pos['market_value']}"
            )
        logger.info(
            f"SPX Long Contracts Counted: {position_stats['spx_long_contracts']}, SPX Short Contracts Counted: {position_stats['spx_short_contracts']}"
        )

    # Part 2: Analyze orders and enrich with current price data
    if isinstance(orders_list, list):
        for order in orders_list:
            if not isinstance(order, dict):
                continue
            try:
                symbol = get_order_symbol(order)
                is_spx = symbol and 'SPX' in symbol.upper()
                order_type = order.get('orderType', '').upper()
                status = order.get('status', '').upper()
                
                if status not in ('WORKING', 'PENDING_ACTIVATION', 'ACCEPTED', 'QUEUED'):
                    continue
                
                # When attaching current price to orders, add a flag if we've adjusted the price
                # This helps UI know how to display it correctly
                if symbol in current_prices and order_type in ('STOP', 'STOP_LIMIT'):
                    order['currentPrice'] = current_prices[symbol]
                    order['isAdjustedPrice'] = is_spx and current_prices[symbol] < 100
                
                if is_spx and order_type in ('STOP', 'STOP_LIMIT'):
                    position_stats["spx_active_stops"] += 1
                    symbols_with_stops.add(symbol)
                
                legs = order.get('orderLegCollection', [])
                if not isinstance(legs, list) or not legs:
                    continue
                    
                for leg in legs:
                    if not isinstance(leg, dict):
                        continue
                    instruction = str(leg.get('instruction', '')).upper()
                    position_effect = str(leg.get('positionEffect', '')).upper()
                    
                    if (is_spx and 'BUY' in instruction and 
                        (position_effect == 'CLOSING' or 'TO_CLOSE' in instruction) and 
                        not order_type.startswith('STOP')):
                        position_stats["spx_closing"] += 1
                        symbols_with_closing.add(symbol)
                        break
            except Exception as e:
                logger.warning(f"Error analyzing order {order.get('orderId', 'N/A')}: {e}")
    
    logger.debug(f"Current prices for {len(current_prices)} symbols: {current_prices}")
    logger.debug(f"SPX symbols with stops: {symbols_with_stops}")
    logger.debug(f"SPX symbols with closing orders: {symbols_with_closing}")
    
    return position_stats


# --- Main Execution ---
if __name__ == '__main__':
    try:
        logger.info("Application starting...")
        load_ignored_items() # Load state before starting monitor

        logger.info("Starting background monitor thread...")
        monitor_thread = threading.Thread(target=background_monitor, name="SchwabMonitorThread", daemon=True)
        monitor_thread.start()

        port = int(os.getenv('PORT', 5001))
        logger.info(f"Starting Flask web server on port {port}...")
        # Use a production-ready server like gunicorn or waitress instead of Flask's built-in server for real use
        # For development:
        app.run(debug=False, host='0.0.0.0', port=port, use_reloader=False) # Disable debug/reloader for stable threading
        # Example using waitress:
        # from waitress import serve
        # serve(app, host='0.0.0.0', port=port)

    except Exception as e:
         logger.critical(f"Application failed to start: {e}", exc_info=True)
