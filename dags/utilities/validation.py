import logging
from psycopg2 import sql

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_duplicates_in_table(conn, table_name):
    """
    This function checks for duplicate values in each column of the specified table
    and prints details of the duplicates found.

    Args:
    conn : psycopg2 connection object
        Connection to the database.
    table_name : str
        Name of the table to check for duplicates.
    """
    try:
        cursor = conn.cursor()
        logging.info(f"Retrieving column names from the table: {table_name}")
        # Retrieve all column names from the table
        cursor.execute(sql.SQL("SELECT * FROM {} LIMIT 0").format(sql.Identifier(table_name)))
        columns = [desc[0] for desc in cursor.description]

        logging.info(f"Checking for duplicates in table: {table_name}")
        has_duplicates = False

        # Check each column for duplicates
        for col in columns:
            logging.info(f"Checking column '{col}' for duplicates")
            # Formulating the SQL query dynamically using psycopg2's SQL module
            query = sql.SQL("""
                SELECT {field}, COUNT(*)
                FROM {table}
                GROUP BY {field}
                HAVING COUNT(*) > 1
            """).format(
                field=sql.Identifier(col),
                table=sql.Identifier(table_name)
            )

            cursor.execute(query)
            duplicates = cursor.fetchall()
            if duplicates:
                has_duplicates = True
                logging.info(f"Column '{col}' has potential duplicates:")
                for value, count in duplicates:
                    logging.info(f"  Value '{value}' appears {count} times")
            else:
                logging.info(f"Column '{col}' has no duplicates.")

        if not has_duplicates:
            logging.info("No duplicate values found across all columns.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
    finally:
        cursor.close()
        logging.info("Database cursor closed")