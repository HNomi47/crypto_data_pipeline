from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
import sys
import os

# --- 1. IMPORT OUR COLLECTOR AND DB TOOLS ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

try:
    from collector_2 import (
        SessionLocal, 
        fetch_live_prices,  
        store_daily_data,   
        COINS_WE_CARE_ABOUT
    )
except ImportError:
    raise ImportError(
        "Could not import 'collector_2'. Make sure 'collector_2.py' "
        "is in the root directory, one level above the 'api' folder."
    )

# --- 2. CREATE THE FASTAPI APP ---
app = FastAPI(
    title="My Crypto API",
    description="A high-speed API to serve crypto data from our Supabase DB."
)

# --- 3. HELPER FUNCTION TO GET A DB SESSION ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- 4. THE "HELLO WORLD" ENDPOINT ---
@app.get("/api/hello")
def get_root():
    return {"message": "Welcome to the Crypto API! It's working!"}

# --- 5. THE *REAL* API ENDPOINT (10-SECOND GATED) ---
@app.get("/api/all-coins")
def get_all_coins_data(db: Session = Depends(get_db)):
    """
    Main endpoint for Streamlit. 
    It calls the collector first (which has a 10s gate) then reads the DB.
    """
    print("--- API Request Received: /api/all-coins ---")
    
    # 1. --- Run the Time-Gated Live Collector ---
    try:
        # Pass 'db' so the collector can save data to your database
        fetch_live_prices(db) 
    except Exception as e:
        print(f"Error during live price fetch: {e}")
    
    # 2. --- Read All Data from Database ---
    try:
        sql_query = text("""
            SELECT
                c.id, c.symbol, c.name, c.image_url,
                c.current_price, c.market_cap, c.total_volume,
                c._24h_percent_change, c.last_updated,
                json_agg(
                    json_build_object(
                        'date', dp.date,
                        'price', dp.price
                    ) ORDER BY dp.date DESC
                ) as "historicalData"
            FROM
                coins c
            LEFT JOIN
                daily_prices dp ON c.id = dp.coin_id
            WHERE
                c.symbol IN :symbols_we_care_about
            GROUP BY
                c.id;
        """)
        
        result = db.execute(sql_query, {"symbols_we_care_about": tuple(COINS_WE_CARE_ABOUT)})
        all_coins_data = [dict(row._mapping) for row in result]
        return {
            "message": "All coins data fetched successfully.",
            "allCoinsData": all_coins_data
        }
    except Exception as e:
        print(f"Error reading from database: {e}")
        return {"error": "Could not fetch data from database.", "details": str(e)}

# --- 6. CRON JOB ENDPOINT (DAILY SNAPSHOT ONLY) ---
@app.get("/api/collect-daily-snapshot")
def collect_daily_snapshot(db: Session = Depends(get_db)): 
    """
    This endpoint is called by Vercel Cron at 23:59.
    """
    print("--- DAILY SNAPSHOT CRON JOB RUNNING ---")
    try:
        # Pass 'db' session here as well
        store_daily_data(db)
        return {"message": "Daily snapshot collected successfully."}
    except Exception as e:
        print(f"Error collecting daily snapshot: {e}")
        return {"error": "Failed to collect daily snapshot.", "details": str(e)}
