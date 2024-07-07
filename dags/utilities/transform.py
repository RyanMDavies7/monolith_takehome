import pandas as pd
from pandas import DataFrame
import joblib
import logging
from sklearn.preprocessing import StandardScaler

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def normalize(conn, model_file, source_table):
    try:
        logging.info(f"Fetching data from the table {source_table}")
        # Query to fetch data from the table
        cursor = conn.cursor()
        query = f"SELECT * FROM {source_table};"
        cur = conn.cursor()
        cur.execute(query)
        df = DataFrame(cur.fetchall(), columns=['id', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'V10',
                                                'V11', 'V12', 'V13', 'V14', 'V15', 'V16', 'V17', 'V18', 'V19', 'V20',
                                                'V21', 'V22', 'V23', 'V24', 'V25', 'V26', 'V27', 'V28', 'Amount',
                                                'Class'])

        logging.info("Preprocessing the data")
        # Preprocess the data
        sc = StandardScaler()
        df['Amount'] = sc.fit_transform(pd.DataFrame(df['Amount']))
        X = df.drop('Class', axis=1)

        logging.info(f"Loading the model from {model_file}")
        # Load the model from the provided file and run prediction
        model = joblib.load(model_file)

        logging.info("Running predictions")
        pred = model.predict(X)

        df['prediction'] = pred.tolist()

        logging.info("Closing the database connection")
        # conn.close()

        logging.info("Normalization and prediction completed successfully")
        return df

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
    finally:
        # Ensure the connection is closed in case of an exception
        if conn and not conn.closed:
            conn.close()
            logging.info("Database connection closed in finally block")