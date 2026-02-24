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
    stream = io.StringIO(file_content.decode('utf-8'))
    reader = csv.DictReader(stream)
    
    count = 0
    for row in reader:
        ticker = row.get('Symbol', '').strip().upper()
        
        # QA Filter: Skip empty rows or the 'Total' footer row
        if not ticker or ticker == 'TOTAL' or 'Pending' in ticker:
            continue
            
        # Map Fidelity Columns to your DB Columns
        shares = float(row.get('Quantity', 0))
        # Fidelity uses 'Last Price' for current, but we want our own engine to fetch that.
        # We save 'Cost Basis Per Share' as our PurchasePrice.
        cost_basis = row.get('Cost Basis Per Share', '0').replace('$', '').replace(',', '')
        purchase_price = float(cost_basis) if cost_basis != 'n/a' else 0.0
        
        # Perform the UPSERT (Check if ticker exists)
        cursor.execute("SELECT Ticker FROM Portfolio WHERE Ticker = ?", (ticker,))
        exists = cursor.fetchone()
        
        if exists:
            cursor.execute("""
                UPDATE Portfolio 
                SET Shares = ?, PurchasePrice = ?, LastUpdated = NULL 
                WHERE Ticker = ?
            """, (shares, purchase_price, ticker))
        else:
            cursor.execute("""
                INSERT INTO Portfolio (Ticker, Shares, PurchasePrice, LastUpdated) 
                VALUES (?, ?, ?, NULL)
            """, (ticker, shares, purchase_price))
        
        count += 1
    
    conn.commit()
    return count

def get_cached_or_live_data(ticker, cursor):
    # 1. Check if we have fresh data in the DB
    cursor.execute("""
        SELECT TOP 1 PurchasePrice, category, CachedTrend, LastUpdated 
        FROM Portfolio WHERE Ticker = ?
    """, (ticker,))
    row = cursor.fetchone()

    # If data is less than 6 hours old, return it immediately
    if row and row.LastUpdated:
        time_diff = datetime.datetime.now() - row.LastUpdated
        if time_diff.total_seconds() < 21600:  # 6 hours
            return row.PurchasePrice, row.category, json.loads(row.CachedTrend)

    # 2. If no cache or stale, fetch live
    price, cat, trend = get_exhaustive_data(ticker)

    # 3. Update the cache in the background
    cursor.execute("""
        UPDATE Portfolio 
        SET LastUpdated = ?, CachedTrend = ? , category = ?
        WHERE Ticker = ?
    """, (datetime.datetime.now(), json.dumps(trend), cat, ticker))
    
    return price, cat, trend

def get_exhaustive_data(ticker):
    """Fetches price and categorizes asset dynamically using yfinance and patterns."""
    ticker_upper = ticker.upper()
    current_price = 0.0
    category = "Other/Miscellaneous"
    trend_data = []

    try:
        # 1. Handle Cash Pattern (The 'XX' convention)
        if re.search(r'[A-Z]{3}XX$', ticker_upper):
            # Cash usually doesn't need a trendline, but we return a flat one [1,1,1,1,1,1,1] 
            # so the frontend doesn't crash
            return 1.0, "Cash & Liquidity", [1.0] * 7

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
                cursor.execute("SELECT Ticker, Shares, category, PurchasePrice, PurchaseDate FROM Portfolio")
                rows = cursor.fetchall()
                
                portfolio_list = []
                for row in rows:
                    # -- current_price, category, trend_data = get_exhaustive_data(row.Ticker)
                    current_price, category, trend_data = get_cached_or_live_data(row.Ticker, cursor)
                    logging.info(f"Fetched data for {row.Ticker} - Price: {current_price}, Category: {category}, Trend: {trend_data}")
                    bought_at = float(row.PurchasePrice) if row.PurchasePrice else 0.0
                    shares = float(row.Shares) if row.Shares else 0.0
                    current_price = float(current_price)

                    # Calculate performance
                    gain_loss = round((current_price - bought_at) * shares, 2)

                    portfolio_list.append({
                        "ticker": row.Ticker,
                        "shares": shares,
                        "price_bought": bought_at,
                        "date_added": str(row.PurchaseDate) if row.PurchaseDate else None,
                        "gain_loss": gain_loss,
                        "category": category,  
                        "trend_data": trend_data    # <--- Send this to the frontend
                    })
 
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