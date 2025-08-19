import requests
import sqlite3
import logging
import time
from datetime import datetime

API_URL = "https://opensky-network.org/api/states/all?lamin=6.0&lamax=38.0&lomin=68.0&lomax=97.0"
DB_NAME = "opensky.db"
FETCH_INTERVAL_SECONDS = 600  # 10 minutes

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def log_etl_run(conn, status, records=0, error_msg=None, run_id=None):
    """Logs the start or end of an ETL run."""
    cursor = conn.cursor()
    now_time = datetime.now().isoformat()
    if run_id:
        # Update an existing run record upon completion
        cursor.execute("""
            UPDATE etl_log
            SET end_time = ?, records_processed = ?, status = ?, error_message = ?
            WHERE run_id = ?
        """, (now_time, records, status, error_msg, run_id))
    else:
        # Insert a new run record at the start
        cursor.execute("""
            INSERT INTO etl_log (start_time, status) VALUES (?, ?)
        """, (now_time, status))
        return cursor.lastrowid
    conn.commit()

def fetch_and_append_data():
    """
    Fetches flight data from the OpenSky API, validates it, and inserts it
    into the database, while logging the entire process.
    """
    conn = None
    run_id = None
    
    try:
        conn = sqlite3.connect(DB_NAME)
        # 1. Log the start of the ETL run
        run_id = log_etl_run(conn, status='Running')
        cursor = conn.cursor()

        logging.info(f"Starting ETL Cycle (Run ID: {run_id})...")
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        states = data.get("states", [])
        
        if not states:
            logging.warning("No flight data received in this cycle.")
            log_etl_run(conn, status='Success', records=0, run_id=run_id)
            return

        fetch_time = datetime.utcnow().isoformat()
        records_to_insert = []
        
        # Data validation and record preparation
        for state in (states or []):
            if len(state) >= 17:
                record = (
                    state[0], state[1].strip() if state[1] else None, state[2], state[3], state[4],
                    state[5], state[6], state[7], state[8], state[9],
                    state[10], state[11], ",".join(map(str, state[12])) if state[12] else None, state[13],
                    state[14], state[15], state[16], fetch_time
                )
                records_to_insert.append(record)

        if records_to_insert:
            insert_query = """
                INSERT OR IGNORE INTO opensky_data (
                    icao24, callsign, origin_country, time_position, last_contact,
                    longitude, latitude, baro_altitude, on_ground, velocity,
                    true_track, vertical_rate, sensors, geo_altitude, squawk,
                    spi, position_source, fetch_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.executemany(insert_query, records_to_insert)
            conn.commit()
            
            records_inserted = cursor.rowcount
            logging.info(f"Inserted {records_inserted} new records.")
            # 2. Log the successful completion of the run
            log_etl_run(conn, status='Success', records=records_inserted, run_id=run_id)
        else:
            logging.info("No valid records to insert for this cycle.")
            log_etl_run(conn, status='Success', records=0, run_id=run_id)

    except Exception as e:
        error_message = str(e)
        logging.error(f"ETL cycle failed: {error_message}")
        if conn and run_id:
            # 3. Log the failure and the specific error message
            log_etl_run(conn, status='Failure', error_msg=error_message, run_id=run_id)

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # The number of cycles has been updated to run for 24 hours.
    # (24 hours * 6 cycles/hour = 144 cycles)
    num_cycles = 144 
    
    logging.info(f"Starting OpenSky ETL job for {num_cycles} cycles (24 hours)...")
    
    for i in range(num_cycles):
        logging.info(f"--- Cycle {i+1} of {num_cycles} ---")
        fetch_and_append_data()
        
        # Avoid sleeping on the very last cycle
        if i < num_cycles - 1:
            logging.info(f"Cycle complete. Waiting {FETCH_INTERVAL_SECONDS} seconds...\n")
            time.sleep(FETCH_INTERVAL_SECONDS)
        
    logging.info("ETL session complete.")