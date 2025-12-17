import os
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
import time 

# --- 1. SETUP: API KEYS & DATABASE ---
CRYPTOCOMPARE_API_KEY = "YOUR_API_KEY_HERE"
DATABASE_URL = "YOUR_DATABASE_URL_HERE"

# Define the minimum time between CryptoCompare API calls (in seconds)
COLLECTOR_COOLDOWN_SECONDS = 10 
LAST_RUN_FILE = "/tmp/last_run_timestamp" # Using /tmp for Vercel write access

try:
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
except Exception as e:
    print(f"Database connection error: {e}")

# --- 2. OUR LIST OF COINS ---
COINS_WE_CARE_ABOUT = ["BTC", "ETH", "SOL", "DOGE", "TRUMP", "TETHER", "USDT"]

# --- 3. HELPER FUNCTIONS for Time Gate ---

def get_last_run_time():
    """Reads the last successful run timestamp."""
    try:
        if os.path.exists(LAST_RUN_FILE):
            with open(LAST_RUN_FILE, 'r') as f:
                return float(f.read().strip())
    except Exception:
        pass
    return 0.0

def set_last_run_time(timestamp):
    """Writes the current timestamp to the gate file."""
    try:
        with open(LAST_RUN_FILE, 'w') as f:
            f.write(str(timestamp))
    except Exception as e:
        print(f"Warning: Could not write last run time file: {e}")

# --- 4. THE COLLECTOR FUNCTIONS ---

def fetch_live_prices(db: Session):
    """
    Fetches the live prices but is time-gated to run only every 10 seconds.
    """
    current_time = time.time()
    last_run_time = get_last_run_time()
    
    # --- THE TIME GATE CHECK ---
    if current_time - last_run_time < COLLECTOR_COOLDOWN_SECONDS:
        print(f"Gate Active: Skipping external API call. Using DB data.")
        return 
    
    print("Gate Open: Calling CryptoCompare API...")

    try:
        # Get symbols from DB
        result = db.execute(text("SELECT symbol FROM coins"))
        symbols = [row[0] for row in result]
        
        if not symbols:
            return

        batch_string = ",".join(symbols)
        API_URL = f"https://min-api.cryptocompare.com/data/pricemultifull?fsyms={batch_string}&tsyms=USD"
        headers = {"authorization": f"Apikey {CRYPTOCOMPARE_API_KEY}"}

        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json().get("RAW", {})

        for symbol, coin_data in data.items():
            price_info = coin_data.get("USD", {})
            sql = text("""
                UPDATE coins
                SET 
                    current_price = :price,
                    market_cap = :mktcap,
                    total_volume = :vol,
                    _24h_percent_change = :pctchange,
                    last_updated = NOW()
                WHERE symbol = :symbol;
            """)
            db.execute(sql, {
                "price": price_info.get("PRICE"),
                "mktcap": price_info.get("MKTCAP"),
                "vol": price_info.get("TOTALVOLUME24HTO"),
                "pctchange": price_info.get("CHANGEPCT24HOUR"),
                "symbol": symbol
            })
        
        db.commit() 
        set_last_run_time(current_time) # Update the gate after success
        print(f"Successfully updated prices for {len(symbols)} coins.")
        
    except Exception as e:
        db.rollback()
        print(f"Error in fetch_live_prices: {e}")

def store_daily_data(db: Session):
    """Saves a daily snapshot of prices."""
    try:
        result = db.execute(text("SELECT id, current_price FROM coins WHERE current_price IS NOT NULL"))
        for coin in result:
            sql = text("""
                INSERT INTO daily_prices (coin_id, date, price)
                VALUES (:coin_id, NOW(), :price)
                ON CONFLICT (coin_id, date) DO NOTHING; 
            """)
            db.execute(sql, {"coin_id": coin[0], "price": coin[1]})
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"Error in store_daily_data: {e}")
