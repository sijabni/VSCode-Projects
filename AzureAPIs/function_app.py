import os
import logging
import azure.functions as func
import json
import pyodbc
import yfinance as yf

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
    
def get_current_price(ticker):
    try:
        stock = yf.Ticker(ticker)
        current_price = stock.info.get("currentPrice", 0.0)
        logging.info(f"Fetched current price for {ticker}: {current_price}")
        return current_price
    except Exception as e:
        logging.error(f"Error fetching price for {ticker}: {str(e)}")
        return 0.0

@app.route(route="get_portfolio", methods=["GET","POST","PUT","DELETE"])
def get_assets(req: func.HttpRequest) -> func.HttpResponse:
#def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    #l    Parse JSON safely
    logging.info("Processing request...")   
    try:
       req_body = req.get_json()
    except ValueError:
       req_body = {}
    logging.info(f"Received request body: {req_body}")

    # 2. Get Connection String correctly
    conn_str = os.environ.get("AzureSqlConnectionString")
    logging.info(f"Retrieved connection string: {'Found' if conn_str else 'Not Found'}")
    conn = pyodbc.connect(conn_str)
    logging.info("Database connection established successfully.")

    if not conn_str:
        return func.HttpResponse("Connection string missing.", status_code=500)
    else:
        logging.info("Connection string retrieved successfuwy.")   
    
        conn = None
    try:
        # 3. Use 'with' to auto-close the connection
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            # --HANDLE GET REQUESTS--
            if req.method == "GET":
                cursor.execute("SELECT Ticker, Shares, PurchasePrice FROM Portfolio")
                rows = cursor.fetchall()
                logging.info(f"Fetched {len(rows)} rows from the database.")
                
                portfolio_list = []
                for row in rows:
                    current_price = get_current_price(row.Ticker)
                    # Safe conversion
                    bought_at = float(row.PurchasePrice) if row.PurchasePrice else 0.0
                    shares = float(row.Shares) if row.Shares else 0.0
                    gain_loss = round((current_price - bought_at) * shares, 2)
                    
                    portfolio_list.append({
                        "ticker": row.Ticker,
                        "shares": shares,
                        "gain_loss": gain_loss
                    })
                    logging.info(f"Processed row: Ticker={row.Ticker}, Shares={shares}, PurchasePrice={bought_at}, Gain/Loss={gain_loss}")
                    

                return func.HttpResponse(
                    json.dumps(portfolio_list),
                    mimetype="application/json",
                    status_code=200
                )
            
            # -- HANDLE POST REQUESTS--
            elif req.method == "POST":
                ticker = req_body.get("ticker")
                shares = req_body.get("shares")
                purchase_price = req_body.get("purchase_price")
                logging.info(f"Received POST data: Ticker={ticker}, Shares={shares}, PurchasePrice={purchase_price}")   

                if not all([ticker, shares, purchase_price]):
                    return func.HttpResponse("Missing required fields.", status_code=400)
                
                cursor.execute(
                    "INSERT INTO Portfolio (Ticker, Shares, PurchasePrice) VALUES (?, ?, ?)",
                    (ticker, shares, purchase_price)
                )
                conn.commit()
                logging.info(f"Inserted new asset: Ticker={ticker}, Shares={shares}, PurchasePrice={purchase_price}")
                
                return func.HttpResponse("Asset added successfully.", status_code=201)
            
            # -- HANDLE PUT REQUESTS--
            elif req.method == "PUT":
                ticker = req_body.get("ticker")
                shares = req_body.get("shares")
                purchase_price = req_body.get("purchase_price")
                logging.info(f"Received PUT data: Ticker={ticker}, Shares={shares}, PurchasePrice={purchase_price}")

                if not ticker:
                    return func.HttpResponse("Ticker is required for update.", status_code=400)
                
                cursor.execute(
                    "UPDATE Portfolio SET Shares = ?, PurchasePrice = ? WHERE Ticker = ?",
                    (shares, purchase_price, ticker)
                )
                conn.commit()
                logging.info(f"Updated asset: Ticker={ticker}, Shares={shares}, PurchasePrice={purchase_price}")
                
                return func.HttpResponse("Asset updated successfully.", status_code=200)
            
            elif req.method == "DELETE":
                ticker = req_body.get("ticker")
                logging.info(f"Received DELETE data: Ticker={ticker}")

                if not ticker:
                    return func.HttpResponse("Ticker is required for deletion.", status_code=400)

                cursor.execute("DELETE FROM Portfolio WHERE Ticker = ?", (ticker,))
                conn.commit()
                logging.info(f"Deleted asset: Ticker={ticker}")

                return func.HttpResponse("Asset deleted successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Database error: {str(e)}")
        return func.HttpResponse("Error accessing database.", status_code=500)
    
    finally:
        # 5. THE FIX: Explicitly close the connection
        if conn:
            conn.close() 