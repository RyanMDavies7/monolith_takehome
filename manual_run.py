from dags.utilities.ingest import csv_ingest
from dags.utilities.connectors import get_db_connection
from dags.utilities.validation import check_duplicates_in_table
from dags.utilities.transform import normalize


if __name__ == '__main__':
    conn = get_db_connection()
    csv_ingest('dags/static/data/creditcard_2023.csv', conn, 'transactions', 'credit3')
    # check_duplicates_in_table(conn, 'credit')
    predictions = normalize(conn, 'dags/static/model/credit_card_model.pkl', 'transactions.credit')
    conn.close()
    predictions.to_csv('dags/static/data/credit_prediction.csv')
