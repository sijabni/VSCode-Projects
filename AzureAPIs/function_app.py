import os
import logging
import azure.functions as func
import json
import pyodbc
import yfinance as yf
import re

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

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
                cursor.execute("SELECT Ticker, Shares, PurchasePrice, PurchaseDate FROM Portfolio")
                rows = cursor.fetchall()
                
                portfolio_list = []
                for row in rows:
                    current_price, category, trend_data = get_exhaustive_data(row.Ticker)
                    logging.info(f"Fetched data for {row.Ticker} - Price: {current_price}, Category: {category}, Trend: {trend_data}")
                    bought_at = float(row.PurchasePrice) if row.PurchasePrice else 0.0
                    shares = float(row.Shares) if row.Shares else 0.0
                    
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
            elif req.method == "POST":
                ticker = req_body.get("ticker")
                shares = req_body.get("shares")
                purchase_price = req_body.get("purchase_price")
                purchase_date = req_body.get("purchase_date") # New Field
                logging.info(f"Adding asset: {ticker}, Shares: {shares}, Price: {purchase_price}, Date: {purchase_date}")

                if not all([ticker, shares]):
                    return func.HttpResponse("Missing ticker or shares.", status_code=400)
                
                cursor.execute(
                    "INSERT INTO Portfolio (Ticker, Shares, PurchasePrice, PurchaseDate) VALUES (?, ?, ?, ?)",
                    (ticker, shares, purchase_price, purchase_date)
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