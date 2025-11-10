# import os
# import requests
# from sqlalchemy import create_engine, text
# from sqlalchemy.orm import sessionmaker

# # --- 1. SETUP: API KEYS & DATABASE (This is all correct) ---
# CRYPTOCOMPARE_API_KEY = "a0cc0ac0cb99a25692393590091d4e4ec419c9108431fcd5a1e84d7fea2abf42"
# DATABASE_URL = "postgresql://postgres.hchyulzbfektgtzioghs:rasulparkn61@aws-1-ap-southeast-2.pooler.supabase.com:5432/postgres"

# try:
#     engine = create_engine(DATABASE_URL)
#     SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
#     print("Database connection successful!")
# except Exception as e:
#     print(f"Database connection error: {e}")
#     exit(1)

# # --- 2. THE SMART "MASTER LIST" FUNCTION ---

# # !!! --- THIS IS YOUR SMART IDEA --- !!!
# # We only care about these 6 symbols.
# COINS_WE_CARE_ABOUT = ["BTC", "ETH", "SOL", "DOGE", "TRUMP", "TETHER"]
# # Note: "TETHER" is probably "USDT". Let's add that too just in case.
# COINS_WE_CARE_ABOUT = ["BTC", "ETH", "SOL", "DOGE", "TRUMP", "TETHER", "USDT"]


# def fetch_master_coin_list():
#     """
#     Fetches the master list, but *only* inserts the 6 coins we care about.
#     This will be lightning fast.
#     """
#     API_URL = "https://min-api.cryptocompare.com/data/all/coinlist"
#     headers = {"authorization": f"Apikey {CRYPTOCOMPARE_API_KEY}"}
    
#     try:
#         response = requests.get(API_URL, headers=headers)
#         response.raise_for_status()
        
#         data = response.json().get("Data", {})
#         print(f"Fetched {len(data)} coins from the master list... searching for our {len(COINS_WE_CARE_ABOUT)} coins.")
        
#         db = SessionLocal()
        
#         # This loop is fast (in memory)
#         for coin_id, coin_info in data.items():
            
#             # !!! --- THIS IS THE FIX --- !!!
#             # Only proceed if the symbol is in our list
#             if coin_info['Symbol'] in COINS_WE_CARE_ABOUT:
                
#                 print(f"Found {coin_info['Symbol']}! Inserting into database...")
                
#                 # This is the "UPSERT" command
#                 sql = text("""
#                     INSERT INTO coins (id, symbol, name, image_url)
#                     VALUES (:id, :symbol, :name, :image_url)
#                     ON CONFLICT (id) DO UPDATE SET
#                         name = EXCLUDED.name,
#                         image_url = EXCLUDED.image_url;
#                 """)
                
#                 # This now only runs 6 times!
#                 db.execute(sql, {
#                     "id": coin_info['Id'],
#                     "symbol": coin_info['Symbol'],
#                     "name": coin_info['CoinName'],
#                     "image_url": f"https://www.cryptocompare.com{coin_info.get('ImageUrl', '')}"
#                 })
        
#         db.commit() # Save the 6 changes
#         print("Successfully populated 'coins' table with our 6 coins.")
        
#     except Exception as e:
#         db.rollback() 
#         print(f"Error in fetch_master_coin_list: {e}")
#     finally:
#         db.close() 

# def fetch_live_prices():
#     """
#     Fetches the live prices for all coins we have in our database.
#     (This function is now also super fast since we only have 6 coins)
#     """
#     db = SessionLocal()
    
#     try:
#         result = db.execute(text("SELECT symbol FROM coins"))
#         symbols = [row[0] for row in result]
        
#         if not symbols:
#             print("No symbols found in database. Run fetch_master_coin_list() first.")
#             return

#         batch_string = ",".join(symbols)
        
#         API_URL = f"https://min-api.cryptocompare.com/data/pricemultifull?fsyms={batch_string}&tsyms=USD"
#         headers = {"authorization": f"Apikey {CRYPTOCOMPARE_API_KEY}"}

#         response = requests.get(API_URL, headers=headers)
#         response.raise_for_status()
        
#         data = response.json().get("RAW", {})

#         for symbol, coin_data in data.items():
#             price_info = coin_data.get("USD", {})
            
#             sql = text("""
#                 UPDATE coins
#                 SET 
#                     current_price = :price,
#                     market_cap = :mktcap,
#                     total_volume = :vol,
#                     _24h_percent_change = :pctchange,
#                     last_updated = NOW()
#                 WHERE symbol = :symbol;
#             """)
            
#             db.execute(sql, {
#                 "price": price_info.get("PRICE"),
#                 "mktcap": price_info.get("MKTCAP"),
#                 "vol": price_info.get("TOTALVOLUME24H"),
#                 "pctchange": price_info.get("CHANGEPCT24HOUR"),
#                 "symbol": symbol
#             })
        
#         db.commit() # Save all 6 price updates
#         print(f"Successfully updated live prices for {len(symbols)} coins.")
        
#     except Exception as e:
#         db.rollback()
#         print(f"Error in fetch_live_prices: {e}")
#     finally:
#         db.close()

# # # --- 3. RUN THE SCRIPT ---
# # if __name__ == "__main__":
# #     print("--- Running SMART Collector Script ---")
    
# #     # 1. Run this to populate our 6 coins (takes ~3 seconds)
# #     fetch_master_coin_list() 
    
# #     # 2. Run this to update their live prices (takes ~1 second)
# #     fetch_live_prices()
    
# #     print("--- Collector Script Finished ---")

#     # --- 3. RUN THE SCRIPT ---
# if __name__ == "__main__":
#     print("--- Running SMART Collector Script (Live Prices ONLY) ---")
    
#     # 1. We already ran this, so we can comment it out.
#     # fetch_master_coin_list() 
    
#     # 2. Now we only run the part that failed.
#     fetch_live_prices()
    
#     print("--- Collector Script Finished ---")

#     # --- ADD THIS NEW FUNCTION TO YOUR SCRIPT ---

# def store_daily_data():
#     """
#     This is Machine #2. It takes a "snapshot" of the current price
#     from the 'coins' table and saves it to the 'daily_prices' table.
#     This function should be run ONCE per day (e.g., at 11:59 PM).
#     """
    
#     db = SessionLocal()
    
#     try:
#         print("--- Running Daily Snapshot Collector (Machine #2) ---")
        
#         # 1. Get all coins from our 'coins' table
#         result = db.execute(text("SELECT id, current_price FROM coins WHERE current_price IS NOT NULL"))
#         coins_to_snapshot = [row for row in result]

#         if not coins_to_snapshot:
#             print("No coins with prices found in 'coins' table. Run fetch_live_prices() first.")
#             return

#         print(f"Found {len(coins_to_snapshot)} coins to snapshot.")

#         # 2. Loop through them and INSERT into 'daily_prices'
#         for coin in coins_to_snapshot:
#             coin_id = coin[0]
#             current_price = coin[1]
            
#             # This SQL command inserts the snapshot.
#             # "ON CONFLICT" ensures we don't save two prices for the same coin on the same day.
#             sql = text("""
#                 INSERT INTO daily_prices (coin_id, date, price)
#                 VALUES (:coin_id, NOW(), :price)
#                 ON CONFLICT (coin_id, date) DO NOTHING; 
#             """)
            
#             db.execute(sql, {
#                 "coin_id": coin_id,
#                 "price": current_price
#             })
            
#         db.commit() # Save all snapshot changes
#         print(f"Successfully saved {len(coins_to_snapshot)} price snapshots to 'daily_prices' table.")
        
#     except Exception as e:
#         db.rollback()
#         print(f"Error in store_daily_data: {e}")
#     finally:
#         db.close()

import os
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# --- 1. SETUP: API KEYS & DATABASE ---
# These are your working, correct keys.
CRYPTOCOMPARE_API_KEY = "a0cc0ac0cb99a25692393590091d4e4ec419c9108431fcd5a1e84d7fea2abf42"
DATABASE_URL = "postgresql://postgres.hchyulzbfektgtzioghs:rasulparkn61@aws-1-ap-southeast-2.pooler.supabase.com:5432/postgres"

try:
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    print("Database connection successful!")
except Exception as e:
    print(f"Database connection error: {e}")
    exit(1)

# --- 2. OUR LIST OF COINS ---
# We only care about these symbols.
COINS_WE_CARE_ABOUT = ["BTC", "ETH", "SOL", "DOGE", "TRUMP", "TETHER", "USDT"]

# --- 3. THE COLLECTOR FUNCTIONS ---

def fetch_master_coin_list():
    """
    Fetches the master list, but *only* inserts the coins we care about.
    """
    API_URL = "https://min-api.cryptocompare.com/data/all/coinlist"
    headers = {"authorization": f"Apikey {CRYPTOCOMPARE_API_KEY}"}
    
    try:
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        
        data = response.json().get("Data", {})
        print(f"Fetched {len(data)} coins from the master list... searching for our coins.")
        
        db = SessionLocal()
        
        for coin_id, coin_info in data.items():
            if coin_info['Symbol'] in COINS_WE_CARE_ABOUT:
                print(f"Found {coin_info['Symbol']}! Inserting into database...")
                sql = text("""
                    INSERT INTO coins (id, symbol, name, image_url)
                    VALUES (:id, :symbol, :name, :image_url)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        image_url = EXCLUDED.image_url;
                """)
                db.execute(sql, {
                    "id": coin_info['Id'],
                    "symbol": coin_info['Symbol'],
                    "name": coin_info['CoinName'],
                    "image_url": f"https://www.cryptocompare.com{coin_info.get('ImageUrl', '')}"
                })
        
        db.commit()
        print("Successfully populated 'coins' table with our chosen coins.")
        
    except Exception as e:
        db.rollback() 
        print(f"Error in fetch_master_coin_list: {e}")
    finally:
        db.close() 

def fetch_live_prices():
    """
    Fetches the live prices for all coins we have in our database.
    This is Machine #1.
    """
    db = SessionLocal()
    
    try:
        result = db.execute(text("SELECT symbol FROM coins"))
        symbols = [row[0] for row in result]
        
        if not symbols:
            print("No symbols found in database. Run fetch_master_coin_list() first.")
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
        print(f"Successfully updated live prices for {len(symbols)} coins.")
        
    except Exception as e:
        db.rollback()
        print(f"Error in fetch_live_prices: {e}")
    finally:
        db.close()

# --- THIS IS THE NEW FUNCTION YOU ARE ADDING ---
def store_daily_data():
    """
    This is Machine #2. It takes a "snapshot" of the current price
    from the 'coins' table and saves it to the 'daily_prices' table.
    """
    db = SessionLocal()
    
    try:
        print("--- Running Daily Snapshot Collector (Machine #2) ---")
        
        result = db.execute(text("SELECT id, current_price FROM coins WHERE current_price IS NOT NULL"))
        coins_to_snapshot = [row for row in result]

        if not coins_to_snapshot:
            print("No coins with prices found in 'coins' table. Run fetch_live_prices() first.")
            return

        print(f"Found {len(coins_to_snapshot)} coins to snapshot.")

        for coin in coins_to_snapshot:
            coin_id = coin[0]
            current_price = coin[1]
            
            sql = text("""
                INSERT INTO daily_prices (coin_id, date, price)
                VALUES (:coin_id, NOW(), :price)
                ON CONFLICT (coin_id, date) DO NOTHING; 
            """)
            
            db.execute(sql, {
                "coin_id": coin_id,
                "price": current_price
            })
            
        db.commit()
        print(f"Successfully saved {len(coins_to_snapshot)} price snapshots to 'daily_prices' table.")
        
    except Exception as e:
        db.rollback()
        print(f"Error in store_daily_data: {e}")
    finally:
        db.close()

# --- 4. RUN THE SCRIPT ---
# This block tells Python what function to run when you type "python collector_2.py"
if __name__ == "__main__":
    
    print("--- Running Collector Script (Daily Snapshot ONLY) ---")
    
    # We comment these out because we don't need them for this test
    # fetch_master_coin_list() 
    fetch_live_prices()
    
    # We ONLY run our new "snapshot" function
    store_daily_data()
    
    print("--- Collector Script Finished ---")