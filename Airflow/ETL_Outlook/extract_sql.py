from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import pyodbc
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

# Path to the SQL file
sql_file_path = '/opt/airflow/sql/Sony_extract.sql'
csv_output_path = '/opt/airflow/output/query_result.csv.gz'  # Output CSV path


# Function to extract SQL query result to CSV
def extract_sql_to_gz():
    # Set up a connection to SQL Server using pyodbc
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                          'SERVER=aqoa-db-test.aqoa.local;'
                          'DATABASE=AQOA_Core;'
                          'UID=AQOA_Core;'
                          'PWD=AQOA_Core*+628')

    # Read the SQL query from the file
    with open(sql_file_path, 'r') as sql_file:
        sql_query = sql_file.read()

    # Execute the query and save the result to a DataFrame
    df = pd.read_sql(sql_query, conn)
    df = df.applymap(lambda x: str(x).replace('.', ',') if isinstance(x, (float, int)) else x)

    # Write the DataFrame to CSV
    df.to_csv(csv_output_path, index=False, compression='gzip')

def send_email_with_attachment(smtp_server, smtp_port, smtp_user, smtp_password, to_email, subject, body, attachment_path):
    try:
        # Set up the MIME
        message = MIMEMultipart()
        message['From'] = smtp_user
        message['To'] = to_email
        message['Subject'] = subject

        # Add body to the email
        message.attach(MIMEText(body, 'plain'))

        # Attach the CSV file
        with open(attachment_path, 'rb') as file:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(attachment_path)}')
            message.attach(part)

        # Set up the SMTP server
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)

        # Send the email
        server.sendmail(smtp_user, to_email, message.as_string())
        server.quit()

        print(f"Email sent successfully to {to_email}")
    except Exception as e:
        print(f"Failed to send email: {e}")


# Define the DAG
with DAG(
        'SONY_EXTRACT',
        default_args=default_args,
        schedule='@once',
        catchup=False
) as dag:
    # Task to extract query result to CSV
    extract_task = PythonOperator(
        task_id='extract_sql_to_csv',
        python_callable=extract_sql_to_gz,
        execution_timeout = timedelta(minutes=30)
    )
    # Task to send an email with the CSV file attached
    send_email_task = PythonOperator(
        task_id='send_email',
        python_callable=send_email_with_attachment,
        op_kwargs={
            'smtp_server': 'auth.smtp.1and1.fr',  # Replace with the actual SMTP server
            'smtp_port': 587,  # SMTP port (587 for TLS)
            'smtp_user': 'ayoub@aqoa.fr',  # Sender email address
            'smtp_password': 'bawjug-wimJyb-6bamre',  # Email account password
            'to_email': 'ayoub@aqoa.fr',  # Recipient email address
            'subject': 'Airflow Data Processing Results',  # Email subject
            'body': 'Please find attached the results of the data processing.',  # Email body
            'attachment_path': '/opt/airflow/output/query_result.csv.gz'  # Path to the CSV file
        }
    )

    extract_task >> send_email_task
