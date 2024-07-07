# Credit Card Fraud Detection Pipeline

This repository contains the implementation of a data pipeline for detecting credit card fraud using a logistic regression model. The pipeline is orchestrated using Apache Airflow and includes tasks for loading, cleaning, predicting, and exporting data.

## Notes for Monolith

### If I had more time I would resolve the following issues...

1. Whilst engaging with the task I encountered some issues with my local Airflow installation that was taking a while to debug
after installing package dependencies so unfortunately I couldn't test my DAG run. It kept saying ERROR: You need to upgrade the database. Please run `airflow db upgrade` but this didn't resolve the issue, i created a manual_run.py file to run the tasks locally.
2. The check_duplicates_in_table() function doesn't return any results from the db which is a little strange. I manually checked the dataset and I couldn't recognise any duplicates so I moved on as I needed to complete the rest of the tasks.


## Overview

The goal of this project is to create an automated data pipeline that processes raw credit card transaction data and outputs predictions of fraudulent transactions. The pipeline consists of the following steps:

1. **Data Loading**: Load the credit card transaction data from a CSV file into a PostgreSQL database.
2. **Data Cleaning**: Clean the data by removing duplicates and normalizing the `Amount` column.
3. **Model Prediction**: Load a pre-trained logistic regression model and use it to predict fraudulent transactions.
4. **Data Export**: Export the prediction results to a CSV file.

## Requirements

- **Python**: All scripts are written in Python.
- **Database**: PostgreSQL is used to store and manage data.
- **Orchestration Tool**: Apache Airflow is used to manage the workflow.

## Setup

### Prerequisites

1. **PostgreSQL**: Ensure that PostgreSQL is installed and running on your system.
2. **Python**: Install Python 3.8 or later.
3. **Airflow**: Install Apache Airflow.

### Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/RyanMDavies7/monolith_takehome
    ```

2. Put the Kaggle dataset into the data folder dags/static/data

3. Create and activate a virtual environment:

    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

4. Install the required Python packages (note you may need to switch between psycopg2 or psycopg2-binary depending on env):

    ```sh
    pip install -r requirements.txt
    ```

5. Set up Airflow:

### Database Configuration

1. Create a PostgreSQL database and user:

    ```sql
    CREATE DATABASE monolith;
    CREATE USER admin WITH PASSWORD 'password';
    GRANT ALL PRIVILEGES ON DATABASE monolith TO admin;
    ```

2. Update the database connection settings in the secrets.py (located in `dags/static/env/secrets.py`):

### Running the Pipeline

1. Start the local Airflow web server and scheduler:

2. Access the Airflow web interface at [http://localhost:8080](http://localhost:8080) and trigger the DAG named `credit_card_processing`.

## Project Structure

- **main.py**: Contains the Airflow DAG and task definitions.
- **dags/static/data/**: Directory for storing input and output data files.
- **dags/utilities/**: Contains utility scripts for data ingestion, transformation, and validation.
- **dags/static/model/**: Contains the pre-trained logistic regression model file (`credit_card_model.pkl`).

## Task Descriptions

1. **get_db_connection_task()**: Establishes a connection to the PostgreSQL database.
2. **data_loading_task()**: Loads data from `creditcard_2023.csv` into the database.
3. **data_cleaning_task()**: Cleans the data by removing duplicates and normalizing the `Amount` column.
4. **model_prediction_task()**: Loads the pre-trained model and makes predictions on the cleaned data.
5. **data_export_task()**: Exports the prediction results to a CSV file.

## References

- [Credit Card Fraud Detection Dataset](https://www.kaggle.com/datasets/nelgiriyewithana/credit-card-fraud-detection-dataset-2023)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)

## Notes

- Ensure that PostgreSQL is running and accessible before starting the Airflow tasks.
- Modify database connection details as per your setup.
- This project assumes basic familiarity with Python, SQL, and Airflow.
