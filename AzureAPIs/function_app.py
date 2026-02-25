import os
import logging
import azure.functions as func
import json
import pyodbc
import yfinance as yf
import re
import datetime
import csv
import io

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

import datetime
import json

def process_fidelity_csv(file_content, cursor, conn):
    # Convert bytes to string buffer
    stream = io.StringIO(file_content.decode('utf-8-sig'))  # Handle potential BOM
    reader = csv.DictReader(stream)

    count = 0
    for row in reader:
        raw_ticker = row.get('Symbol', '')
        ticker = row.get('Symbol', '').strip().upper()
        logging.info(f"Processing ticker: {ticker}")
        # QA Filter: Skip empty rows or the 'Total' footer row
        if not ticker or ticker == 'SYMBOL' or 'PENDING' in ticker:
            continue

        try:
            # 2. Robust Numeric Parsing (Handles empty strings and commas)
            raw_qty = row.get('Quantity', '').strip()
            raw_cost = row.get('Average Cost Basis', '0').replace('$', '').replace(',', '').strip()
            raw_last_price = row.get('Last Price', '0').replace('$', '').replace(',', '').strip()
            raw_value = row.get('Current Value', '0').replace('$', '').replace(',', '').strip()

            if not raw_qty or raw_qty == '--' or ticker.endswith('XX'):
                shares = float(raw_qty) if raw_qty and raw_qty != '--' else 0.0
                purchase_price = float(raw_cost) if raw_cost and raw_cost != '--' and raw_cost != 'n/a' else 0.0
                current_price = 1.0  # For cash, we treat it as $1 per unit
            else:
                shares = float(raw_qty.replace(',', ''))
                purchase_price = float(raw_cost.replace('$', '').replace(',', '')) if raw_cost and raw_cost != '--' else 0.0
                current_price = float(raw_last_price) if raw_last_price and raw_last_price != '--' else 0.0 
                logging.info(f"Processing {ticker} - Shares: {shares}, Purchase Price: {purchase_price}")
                       
            # Perform the UPSERT (Check if ticker exists)
            cursor.execute("SELECT Ticker FROM Portfolio WHERE Ticker = ?", (ticker,))
            exists = cursor.fetchone()
            
            if exists:
                cursor.execute("""
                    UPDATE Portfolio 
                    SET Shares = ?, PurchasePrice = ?, CurrentPrice = ?, LastUpdated = NULL 
                    WHERE Ticker = ?
                """, (shares, purchase_price, current_price, ticker))
            else:
                cursor.execute("""
                    INSERT INTO Portfolio (Ticker, Shares, PurchasePrice, CurrentPrice, LastUpdated) 
                    VALUES (?, ?, ?, ?, NULL)
                """, (ticker, shares, purchase_price, current_price))
            
            count += 1
            logging.info(f"Processed {ticker}: {shares} shares")

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
    ticker_upper = ticker.upper()
    current_price = 0.0
    category = "Other/Miscellaneous"
    trend_data = []

    try:
        # 1. Handle Cash Pattern (The 'XX**' convention)
        if re.search(r'[A-Z]{3}XX[\*]*$', ticker_upper):
            # Cash usually doesn't need a trendline, but we return a flat one [1,1,1,1,1,1,1] 
            # so the frontend doesn't crash
            return 1.0, "Cash & Liquidity", [1.0] * 7

        yf_ticker = ticker_upper.replace('*', '').replace('$', '')
        if yf_ticker == "BRKB":
            yf_ticker = "BRK-B"

        stock = yf.Ticker(ticker)
        info = stock.info
        
        # 2. Fetch Trend Data (7 days)
        hist = stock.history(period="7d", interval="1d")
        trend_data = hist['Close'].fillna(0).tolist() if not hist.empty else []

        # 3. Determine Price
        current_price = info.get("currentPrice", info.get("navPrice", 0.0))
        if current_price == 0 and trend_data:
            current_price = trend_data[-1]

        # 4. Categorization Logic
        cash_equivalents = ['BIL', 'SGOV', 'CLIP', 'SHV', 'VGSH']
        is_cash_etf = ticker_upper in cash_equivalents
        
        if info.get("quoteType") == "ETF":
            if is_cash_etf:
                category = "Cash & Liquidity"
            elif "International" in info.get("longName", ""):
                category = "International Equity"
            else:
                category = "Equity ETFs"
        else:
            sector = info.get("sector")
            if sector:
                if sector in ["Technology", "Communication Services"]:
                    category = "Growth/Tech" 
                else:
                    category = sector

        # THE FIX: Always return all three values
        return current_price, category, trend_data
    
    except Exception as e:
        logging.error(f"Error fetching {ticker}: {str(e)}")
        return 0.0, "Other", []
    
def get_current_price(ticker):
    try:
        stock = yf.Ticker(ticker)
        # Use fast_info or info; fast_info is quicker for basic data
        current_price = stock.info.get("currentPrice", 0.0)
        return current_price
    except Exception as e:
        logging.error(f"Error fetching price for {ticker}: {str(e)}")
        return 0.0
    
@app.route(route="get_portfolio", methods=["GET","POST","PUT","DELETE"])
def get_assets(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(f"Processing {req.method} request...") 

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
                cursor.execute("SELECT Ticker, Shares, category, PurchasePrice, PurchaseDate, CurrentPrice FROM Portfolio")
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

                    num_processed = process_fidelity_csv(file_content, cursor, conn)
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