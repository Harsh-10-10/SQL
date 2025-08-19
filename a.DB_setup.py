import sqlite3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_database(db_name="opensky.db"):
    """
    Sets up the SQLite database, creating the main data table, indexes, 
    and the ETL log table.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS opensky_data (
        icao24 TEXT NOT NULL,
        callsign TEXT,
        origin_country TEXT NOT NULL,
        time_position INTEGER,
        last_contact INTEGER NOT NULL,
        longitude REAL,
        latitude REAL,
        baro_altitude REAL,
        on_ground BOOLEAN NOT NULL,
        velocity REAL,
        true_track REAL,
        vertical_rate REAL,
        sensors TEXT,
        geo_altitude REAL,
        squawk TEXT,
        spi BOOLEAN NOT NULL,
        position_source INTEGER,
        fetch_time TEXT NOT NULL,
        PRIMARY KEY (icao24, last_contact)
    );
    """

    create_country_index_query = "CREATE INDEX IF NOT EXISTS idx_origin_country ON opensky_data (origin_country);"
    create_time_index_query = "CREATE INDEX IF NOT EXISTS idx_last_contact ON opensky_data (last_contact);"

    # Query to create the ETL log table
    create_log_table_query = """
    CREATE TABLE IF NOT EXISTS etl_log (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_time TEXT NOT NULL,
        end_time TEXT,
        records_processed INTEGER,
        status TEXT NOT NULL CHECK(status IN ('Running', 'Success', 'Failure')),
        error_message TEXT
    );
    """

    conn = None
    try:
        logging.info(f"Connecting to database '{db_name}'...")
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        cursor.execute(create_table_query)
        cursor.execute(create_country_index_query)
        cursor.execute(create_time_index_query)
        
        # Execute the log table creation
        cursor.execute(create_log_table_query)

        conn.commit()
        logging.info("Database setup successful. All tables and indexes are ready.")
    except sqlite3.Error as e:
        logging.error(f"Database setup error: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    setup_database()