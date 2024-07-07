import psycopg2
import logging
from dags.static.env.secrets import db_name, db_user, db_password, db_host, db_port

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    try:
        logging.info("Attempting to connect to the database")
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        logging.info("Database connection successful")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise