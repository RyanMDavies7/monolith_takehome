import pandas as pd
import logging
from psycopg2.extras import execute_batch

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def csv_ingest(csv_file_path, conn, schema_name, table_name):
    try:
        logging.info(f"Reading CSV file: {csv_file_path}")
        # Step 1: Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file_path)

        logging.info("Handling NaN values and converting data types")
        # Step 2: Handle NaN values and convert data types to native Python types
        df = df.where(pd.notnull(df), None)

        cur = conn.cursor()

        # Step 4: Create a table
        logging.info(f"Creating table {schema_name}.{table_name}")
        # Generate the CREATE TABLE statement based on DataFrame schema
        create_table_query = f"CREATE TABLE {schema_name}.{table_name} ("

        for col_name, col_type in zip(df.columns, df.dtypes):
            if col_type == "int64":
                create_table_query += f"{col_name} INTEGER, "
            elif col_type == "float64":
                create_table_query += f"{col_name} FLOAT, "
            elif col_type == "object":
                create_table_query += f"{col_name} TEXT, "
            else:
                create_table_query += f"{col_name} TEXT, "

        # Remove the trailing comma and space, and add closing parenthesis
        create_table_query = create_table_query.rstrip(", ") + ");"

        # Execute the create table query
        cur.execute(create_table_query)
        conn.commit()
        logging.info(f"Table {schema_name}.{table_name} created successfully")

        # Step 5: Insert data into the table
        logging.info(f"Inserting data into table {schema_name}.{table_name}")
        # Convert DataFrame to a list of tuples
        data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Generate the INSERT INTO statement
        columns = ', '.join(df.columns)
        values_placeholder = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({values_placeholder})"

        # Use execute_batch for better performance
        execute_batch(cur, insert_query, data_tuples)

        # Commit the transaction
        conn.commit()
        logging.info(f"Data inserted into table {schema_name}.{table_name} successfully")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
    finally:
        # Close the connection
        cur.close()
        # conn.close()
        logging.info("Database connection closed")