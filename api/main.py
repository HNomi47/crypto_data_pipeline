from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
import sys
import os

# --- 1. IMPORT OUR COLLECTOR AND DB TOOLS ---
# We must add 'collector_2' to the system path so Vercel can find it
# Get the absolute path to the project root
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
    # This provides a helpful error message during deployment
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

# --- 5. THE *REAL* API ENDPOINT ---
@app.get("/api/all-coins")
def get_all_coins_data(db: Session = Depends(get_db)):
    """
    This is the main endpoint our Streamlit app will call.
    It follows our "on-demand" logic:
    """
    print("--- API Request Received: /api/all-coins ---")

    # 1. --- Run the Live Collector ---
    try:
        print("Step 1: Fetching live prices...")
        fetch_live_prices()
        print("Step 1: Live prices updated.")
    except Exception as e:
        print(f"Error during live price fetch: {e}")

    # 2. --- Read All Data from Database ---
    try:
        print("Step 2: Reading all data from database...")
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
        print(f"Step 2: Successfully read {len(all_coins_data)} coins from DB.")

        return {
            "message": "All coins data fetched successfully.",
            "allCoinsData": all_coins_data
        }
    except Exception as e:
        print(f"Error reading from database: {e}")
        return {"error": "Could not fetch data from database.", "details": str(e)}

# --- 6. CRON JOB ENDPOINT (This is NEW!) ---
@app.get("/api/collect-daily-snapshot")
def collect_daily_snapshot():
    """
    This endpoint is called by the Vercel cron job once per day.
    It runs our store_daily_data() function.
    """
    print("--- API Request Received: /api/collect-daily-snapshot (Cron Job) ---")
    try:
        store_daily_data()
        print("Daily snapshot collected successfully.")
        return {"message": "Daily snapshot collected successfully."}
    except Exception as e:
        print(f"Error collecting daily snapshot: {e}")
        return {"error": "Failed to collect daily snapshot.", "details": str(e)}