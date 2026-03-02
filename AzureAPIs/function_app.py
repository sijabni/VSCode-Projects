import os
import logging
from unittest import result
import azure.functions as func
import json
import pyodbc
import yfinance as yf
import re
import datetime
import csv
import io
import bcrypt
import jwt

SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "your_default_secret_key")  # In production, ensure this is set securely in your environment variables    

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
conn_str = os.environ.get("AzureSqlConnectionString")
conns = None
if conn_str:
    func.HttpResponse("Connection string missing.", status_code=500)
    try:
        conns = pyodbc.connect(conn_str)
        # This keeps the connection "hot" for all functions below
        logging.info("Global SQL Connection established.")
    except Exception as e:
        logging.error(f"Failed to connect to SQL: {e}")
        conns = None
else:
    logging.error("AzureSqlConnectionString is missing from environment variables.")

def hash_password(password):
    # Salt adds randomness so two "password123" results in different hashes
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

def verify_password(password, hashed_password):
    return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))

def generate_token(user_id,username):
    payload = {
        'user_id': user_id,
        'username': username,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24) # Token expires in 1 day
    }
    return jwt.encode(payload, SECRET_KEY, algorithm='HS256')

def verify_token(req):
    auth_header = req.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return None
    
    token = auth_header.split(" ")[1]
    try:
        decoded = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        return decoded
    except:
        return None

def process_fidelity_csv(file_content, cursor, conn, current_user_id):
    # Convert bytes to string buffer
    cursor.execute("SELECT FidelitySymbol, MarketSymbol FROM SymbolMapping")
    mappings = {row[0]: row[1] for row in cursor.fetchall()}
    
    stream = io.StringIO(file_content.decode('utf-8-sig'))  # Handle potential BOM
    reader = csv.DictReader(stream)

    count = 0
    for row in reader:
        #raw_ticker = row.get('Symbol', '')
        #ticker = row.get('Symbol', '').strip().upper()
        #logging.info(f"Processing ticker: {ticker}")
        # QA Filter: Skip empty rows or the 'Total' footer row

        # CLEANING: Remove stars and dollar signs from the ticker
        raw_sym = row.get('Symbol', '').replace('*', '').replace('$', '').strip().upper()

        # Skip footer/disclaimer rows (if Symbol is empty or too long)
        if not raw_sym or len(raw_sym) > 10 or 'DATE' in raw_sym:
            continue

        # MAPPING: Check if this symbol needs a market-friendly translation (e.g., BRKB -> BRK-B)
        ticker = mappings.get(raw_sym, raw_sym)
        if not ticker or ticker == 'SYMBOL' or 'PENDING' in ticker:
            continue

        try:
            
            # 2. Robust Numeric Parsing (Handles empty strings and commas)
            
            raw_qty = row.get('Quantity', '').strip()
            raw_cost = row.get('Average Cost Basis', '0').strip()
            raw_last_price = row.get('Last Price', '0').replace('$', '').replace(',', '').strip()
            raw_value = row.get('Current Value', '0').replace('$', '').replace(',', '').strip()

            if not raw_qty or raw_qty == '--' or ticker.endswith('XX'):
                # 1 Share = $1.00. Current Value IS the share count.
                shares = float(raw_value) if raw_value else 0.0
                purchase_price = 1.0
                current_price = 1.0
            else:
                shares = float(raw_qty.replace(',', ''))
                purchase_price = float(raw_cost.replace('$', '').replace(',', '')) if raw_cost and raw_cost != '--' else 0.0
                current_price = float(raw_last_price) if raw_last_price and raw_last_price != '--' else 0.0  
                       
            # Perform the UPSERT (Check if ticker exists)
            cursor.execute("SELECT Ticker FROM Portfolio WHERE Ticker = ? AND UserID = ?", (ticker, current_user_id))
            exists = cursor.fetchone()
            
            if exists:
                cursor.execute("""
                    UPDATE Portfolio 
                    SET Shares = ?, PurchasePrice = ?, CurrentPrice = ?, LastUpdated = NULL 
                    WHERE Ticker = ? AND UserID = ?
                """, (shares, purchase_price, current_price, ticker, current_user_id))
            else:
                cursor.execute("""
                    INSERT INTO Portfolio (Ticker, Shares, PurchasePrice, CurrentPrice, UserId, LastUpdated) 
                    VALUES (?, ?, ?, ?, ?, NULL)
                """, (ticker, shares, purchase_price, current_price, current_user_id))
            
            count += 1
            logging.info(f"Processed {ticker} for user {current_user_id}")

        except Exception as e:
                logging.error(f"Failed to process row for {ticker}: {e}")
                continue # Skip this row and keep going instead of crashing
        
    conn.commit()
    return count

def get_cached_or_live_data(ticker, cursor):
    # 1. Check if we have fresh data in the DB
    cursor.execute("""
        SELECT TOP 1 CurrentPrice, PurchasePrice, category, CachedTrend, LastUpdated 
        FROM Portfolio WHERE Ticker = ?
    """, (ticker,))
    row = cursor.fetchone()

    # If data is less than 6 hours old, return it immediately
    if row and row.LastUpdated:
        time_diff = datetime.datetime.now() - row.LastUpdated
        if time_diff.total_seconds() < 21600:  # 6 hours
            return row.CurrentPrice or 0.0, row.category, json.loads(row.CachedTrend)

    # 2. If no cache or stale, fetch live
    price, cat, trend = get_exhaustive_data(ticker)

    # 3. Update the cache in the background
    cursor.execute("""
        UPDATE Portfolio 
        SET LastUpdated = ?, CachedTrend = ? , category = ? , CurrentPrice = ?
        WHERE Ticker = ?
    """, (datetime.datetime.now(), json.dumps(trend), cat, float(price), ticker))
    
    return price, cat, trend

def get_exhaustive_data(ticker):
    """Fetches price and categorizes asset dynamically using yfinance and patterns."""
    # 1. Standardize and Clean the Ticker immediately
    clean_ticker = ticker.replace('*', '').replace('$', '').strip().upper()
    
    current_price = 0.0
    category = "Other/Miscellaneous"
    trend_data = []

    try:
        # 2. Handle Cash Pattern (FDRXX, etc.)
        # We check the CLEANED ticker so we don't need complex regex for stars
        if re.search(r'[A-Z]{3}XX$', clean_ticker):
            return 1.0, "Cash & Liquidity", [1.0] * 7

        # 3. Market Symbol Mapping (Discrepancy Fixes)
        yf_ticker = clean_ticker
        if yf_ticker == "BRKB":
            yf_ticker = "BRK-B"
        # Add others here as needed: if yf_ticker == "BFB": yf_ticker = "BF-B"

        # 4. Use the CLEAN yf_ticker for the market engine
        stock = yf.Ticker(yf_ticker)
        info = stock.info
        
        # 5. Fetch Trend Data (7 days)
        hist = stock.history(period="7d", interval="1d")
        trend_data = hist['Close'].fillna(0).tolist() if not hist.empty else []

        # 6. Determine Current Market Price
        current_price = info.get("currentPrice", info.get("navPrice", 0.0))
        if current_price == 0 and trend_data:
            current_price = trend_data[-1]

        # 7. Categorization Logic
        cash_equivalents = ['BIL', 'SGOV', 'CLIP', 'SHV', 'VGSH']
        
        if yf_ticker in cash_equivalents:
            category = "Cash & Liquidity"
        elif info.get("quoteType") == "ETF":
            if "International" in info.get("longName", ""):
                category = "International Equity"
            else:
                category = "Equity ETFs"
        else:
            sector = info.get("sector", "Other")
            if sector in ["Technology", "Communication Services"]:
                category = "Growth/Tech" 
            else:
                category = sector

        return current_price, category, trend_data
    
    except Exception as e:
        logging.error(f"Error fetching {ticker}: {str(e)}")
        # Return 0.0 so the dashboard shows something, but logs the error
        return 0.0, "Other", []
    
@app.route(route="register", methods=["POST"])
def register(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    username = req_body.get('username')
    password = req_body.get('password')

    if not username or not password:
        return func.HttpResponse("Username and password required", status_code=400)

    # 1. Hash the password using the same method as your login
    # We use gensalt() to ensure every user has a unique, secure hash
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    conn_str = os.environ.get("AzureSqlConnectionString")
    
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            # 2. QA Check: Does this user already exist?
            cursor.execute("SELECT UserID FROM Users WHERE Username = ?", (username,))
            if cursor.fetchone():
                return func.HttpResponse("Username already exists", status_code=409)

            # 3. Insert the new user into the database
            cursor.execute("INSERT INTO Users (Username, PasswordHash) VALUES (?, ?)", 
                           (username, hashed_password))
            conn.commit()
            
        return func.HttpResponse("User created successfully", status_code=201)
    except Exception as e:
        return func.HttpResponse(f"Database error: {str(e)}", status_code=500)

@app.route(route="login", methods=["POST"])
def login(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # 1. Parse the incoming JSON
        req_body = req.get_json()
        username = req_body.get('username')
        password = req_body.get('password')

        logging.info(f"Login attempt for user: {username}") 

        if not username or not password:
            return func.HttpResponse("Missing username or password", status_code=400)

        # 2. Check the database
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT UserID, PasswordHash FROM Users WHERE Username = ?", (username,))
                row = cursor.fetchone()
        except Exception as db_err:
            logging.error(f"Database error during login: {db_err}")
            return func.HttpResponse("Database connection error", status_code=500)

        if row:
            user_id = row[0]
            stored_hash = row[1]
            
            logging.info(f"Checking pass for user: {username}")
            logging.info(f"Input pass (encoded): {password.encode('utf-8')}")
            logging.info(f"Stored hash from DB: {row[1]}")
            
            # Try this more robust comparison:
            stored_hash_bytes = row[1].strip().encode('utf-8')
            result = bcrypt.checkpw(password.encode('utf-8'), stored_hash_bytes)
            logging.info(f"Final Check Result: {result}")

            # 3. Verify the password
            #if bcrypt.checkpw(password.encode('utf-8'), row[1].strip().encode('utf-8')):
            #if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
                # 4. Generate the JWT Token
            if bcrypt.checkpw(password.encode('utf-8'), stored_hash_bytes):
                token = jwt.encode({
                    'user_id': user_id,
                    'username': username,
                    'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)
                }, SECRET_KEY, algorithm='HS256')

                return func.HttpResponse(
                    json.dumps({"token": token, "username": username}),
                    mimetype="application/json",
                    status_code=200
                )

        return func.HttpResponse("Invalid credentials", status_code=401)

    except Exception as e:
        logging.error(f"General login error: {e}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
    
@app.route(route="get_portfolio", methods=["GET","POST","PUT","DELETE"])
def get_assets(req: func.HttpRequest) -> func.HttpResponse:
    
    username = verify_token(req)
    if not username:
        return func.HttpResponse("Unauthorized", status_code=401)
    
    logging.info(f"Processing {req.method} request for user: {username}")
    current_user_id = username.get("user_id")  # Assuming the token contains user_id, adjust if your token structure is different

    # 1. Parse JSON safely
    try:
        req_body = req.get_json()
    except ValueError:
        req_body = {}

    # 2. Connection Setup
    conn_str = os.environ.get("AzureSqlConnectionString")
    if not conn_str:
        return func.HttpResponse("Connection string missing.", status_code=500)
    
    conn = None
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            # -- HANDLE GET: Retrieve Date and Price --
            if req.method == "GET":
                cursor.execute("SELECT Ticker, Shares, category, PurchasePrice, PurchaseDate, CurrentPrice FROM Portfolio WHERE UserID = ?", (current_user_id,))
                rows = cursor.fetchall()
                
                portfolio_list = []
                for row in rows:
                    db_ticker = str(row.Ticker).replace('$', '').replace('*', '').strip().upper()
                    # 2. MAP TO MARKET SYMBOL: yfinance needs 'BRK-B' for Berkshire
                    market_ticker = "BRK-B" if db_ticker == "BRKB" else db_ticker
                    # -- current_price, category, trend_data = get_exhaustive_data(row.Ticker)
                    
                    try:
                        current_price, category, trend_data = get_cached_or_live_data(row.Ticker, cursor)
                        logging.info(f"Fetched data for {row.Ticker} - Price: {current_price}, Category: {category}, Trend: {trend_data}")
                        bought_at = float(row.PurchasePrice) if row.PurchasePrice else 0.0
                        shares = float(row.Shares) if row.Shares else 0.0
                        curr_p = float(current_price) if current_price else 0.0

                        # Calculate performance
                        gain_loss = round((curr_p - bought_at) * shares, 2)
                        logging.info(f"{row.Ticker} - Bought at: {bought_at}, Current: {curr_p}, Shares: {shares}, Gain/Loss: {gain_loss}")

                        portfolio_list.append({
                            "ticker": row.Ticker,
                            "shares": shares,
                            "price_bought": bought_at,
                            "date_added": str(row.PurchaseDate) if row.PurchaseDate else None,
                            "gain_loss": gain_loss,
                            "category": category,  
                            "trend_data": trend_data    # <--- Send this to the frontend
                        })
                    except Exception as e:
                        # If one ticker fails (e.g. delisted stock), don't crash the whole dashboard
                        logging.error(f"Error processing {db_ticker}: {e}")
                        continue

                return func.HttpResponse(json.dumps(portfolio_list), mimetype="application/json", status_code=200)
             
            # -- HANDLE POST: Save New Date Field --
            
            elif req.method == "POST" and req.params.get("action") == "upload":
                try:
                    # Get the file from the request
                    logging.info("Processing file upload...yomama")
                    file = req.files.get('file')
                    if not file:
                        return func.HttpResponse("No file uploaded.", status_code=400)
                    
                    logging.info(f"Received file: {file.filename}, Size: {len(file.stream.read())} bytes")
                    file.stream.seek(0)  # Reset stream position after reading for logging
                    file_content = file.stream.read()
                    logging.info(f"File content read successfully, size: {len(file_content)} bytes")

                    num_processed = process_fidelity_csv(file_content, cursor, conn, current_user_id)
                    logging.info(f"Processed {num_processed} assets from uploaded file.")


                    return func.HttpResponse(f"Success: Processed {num_processed} assets.", status_code=200)
                except Exception as e:
                    return func.HttpResponse(f"Parsing Error: {str(e)}", status_code=500)
            
            elif req.method == "POST":
                ticker = req_body.get("ticker").upper()
                shares = float(req_body.get("shares") or 0)
                purchase_price = float(req_body.get("purchase_price") or 0)
                purchase_date = req_body.get("purchase_date") # New Field
                logging.info(f"Adding asset: {ticker}, Shares: {shares}, Price: {purchase_price}, Date: {purchase_date}")

                if not ticker or shares <= 0:
                    return func.HttpResponse("Invalid ticker or shares.", status_code=400)
                # --- QA FIX: Fetch live data so the cache isn't empty on day one ---
                try:
                    current_price, category, trend_data = get_exhaustive_data(ticker)
                except Exception as e:
                    logging.error(f"Cache priming failed: {e}")
                    category = "Other/Miscellaneous"
                    trend_data = []

                cursor.execute(
                    "INSERT INTO Portfolio (Ticker, Shares, PurchasePrice, PurchaseDate, LastUpdated, CachedTrend, category) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (ticker, shares, purchase_price, purchase_date, datetime.datetime.now(), json.dumps(trend_data), category)
                )
                conn.commit()
                return func.HttpResponse("Asset added successfully.", status_code=201)
            
            # -- HANDLE PUT: Update Date Field --
            elif req.method == "PUT":
                ticker = req_body.get("ticker")
                shares = req_body.get("shares")
                purchase_price = req_body.get("purchase_price")
                purchase_date = req_body.get("purchase_date") # New Field
                logging.info(f"Updating asset: {ticker}, Shares: {shares}, Price: {purchase_price}, Date: {purchase_date}")

                if not ticker:
                    return func.HttpResponse("Ticker required.", status_code=400)
                
                cursor.execute(
                    "UPDATE Portfolio SET Shares = ?, PurchasePrice = ?, PurchaseDate = ? WHERE Ticker = ?",
                    (shares, purchase_price, purchase_date, ticker)
                )
                conn.commit()
                return func.HttpResponse("Asset updated.", status_code=200)
            
            # -- HANDLE DELETE --
            elif req.method == "DELETE":
                # Check for ticker in URL params if not in body
                ticker = req_body.get("ticker") or req.params.get("ticker")
                
                if not ticker:
                    return func.HttpResponse("Ticker required.", status_code=400)

                cursor.execute("DELETE FROM Portfolio WHERE Ticker = ?", (ticker,))
                conn.commit()
                return func.HttpResponse("Asset deleted.", status_code=200)
            
            

    except Exception as e:
        logging.error(f"Database error: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
    
    finally:
        if conn:
            conn.close()