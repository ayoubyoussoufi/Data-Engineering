import imaplib
import email
import os
import json
from email.header import decode_header
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from googleapiclient.errors import HttpError
import traceback

# IMAP server credentials
IMAP_SERVER = 'imap.ionos.fr'
IMAP_USER = 'ayoub@loreal.fr'
IMAP_PASS = 'V@room22'


def setup_task_logger(task_name):
    logger = logging.getLogger(task_name)
    logger.setLevel(logging.INFO)
    
    directory_path = '/opt/airflow/task_id_logs/'
    file_path = os.path.join(directory_path, f'{task_name}.log')
    
    # Create the directory if it does not exist
    os.makedirs(directory_path, exist_ok=True)

    # Clear log file before each run
    open(file_path, 'w').close()
    
    file_handler = logging.FileHandler(file_path)  # Ensure this path matches the volume mount
    formatter = logging.Formatter('%(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

def load_config(file_path, **context):
    task_logger = setup_task_logger('Configuration')
    task_logger.info("==================================================")
    task_logger.info(f"DAG Run Initiated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    task_logger.info("==================================================")
    try:
        task_logger.info("CONFIGURATION STATUS:")
        task_logger.info(f"  - Source: {file_path}")
        with open(file_path, 'r') as file:
            config = json.load(file)
        task_logger.info("  - Status: Loaded Successfully")
        context['task_instance'].xcom_push(key='config', value=config)
    except Exception as e:
        task_logger.error(f"  - Error loading configuration: {e}")
        raise
    task_logger.info("==================================================")

def sanitize_filename(filename):
    """Sanitize the filename to remove or replace invalid characters."""
    return "".join(c if c.isalnum() or c in "._- " else "_" for c in filename)

def get_current_week_info():
    today = datetime.today()
    week_start = today - timedelta(days=today.weekday())
    previous_week_start = week_start - timedelta(weeks=1)
    year, week_number, _ = previous_week_start.isocalendar()
    return year, week_number

def receive_datafiles(**context):
    import logging
    from base64 import b64encode

    task_logger = setup_task_logger('Receiving data files')
    config = context['task_instance'].xcom_pull(key='config', task_ids='load_config')
    task_logger.info("EMAIL PROCESSING SUMMARY:")
    task_logger.info("--------------------------------------------------")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(IMAP_USER, IMAP_PASS)

    folder_name = '"INBOX/MATRIX/Prod hebdo"'
    status, response = mail.select(folder_name)
    #status, response = mail.select('INBOX')
    if status != 'OK':
        raise Exception(f"Failed to select folder: {response}")

    status, messages = mail.search(None, 'UNSEEN')
    if status != 'OK':
        raise Exception(f"Failed to search emails: {messages}")

    email_ids = messages[0].split()
    task_logger.info(f"Total Unseen Emails Found: {len(email_ids)}")
    task_logger.info("--------------------------------------------------")

    email_details = []
    email_log = {}

    for email_id in email_ids:
        status, msg_data = mail.fetch(email_id, '(RFC822)')
        msg = email.message_from_bytes(msg_data[0][1])

        from_address = email.utils.parseaddr(msg['From'])[1]
        task_logger.info(f"Email from: {from_address}")

        attachments = []
        for part in msg.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is not None:
                subject, encoding = decode_header(msg['Subject'])[0]
                if isinstance(subject, bytes):
                    subject = subject.decode(encoding if encoding else 'utf-8')

                filename = part.get_filename()
                if filename:
                    filename, encoding = decode_header(filename)[0]
                    if isinstance(filename, bytes):
                        filename = filename.decode(encoding if encoding else 'utf-8')

                    filename = sanitize_filename(filename)
                    task_logger.info(f"  -> Attachment: {filename}")

                    payload = b64encode(part.get_payload(decode=True)).decode('utf-8')
                    email_details.append((from_address, filename, payload))
                    attachments.append(filename)

        if not attachments:
            task_logger.info("  -> No attachments found.")

        email_log[from_address] = attachments
        task_logger.info("--------------------------------------------------")
        task_logger.info("")  # Add empty line between emails

        mail.store(email_id, '+FLAGS', '\\Seen')

    mail.close()
    mail.logout()

    context['task_instance'].xcom_push(key='email_details', value=email_details)
    task_logger.info(f"Email details: {json.dumps(email_log, indent=2)}")
    task_logger.info("==================================================")
    task_logger.info("")  # Add empty line at the end of the section

def check_patterns(**context):
    import logging
    import json

    task_logger = setup_task_logger('Check Patterns')
    config = context['task_instance'].xcom_pull(key='config', task_ids='load_config')
    email_details = context['task_instance'].xcom_pull(key='email_details', task_ids='receive_datafiles')

    task_logger.info("===============================")
    task_logger.info("Pattern Matching Summary:")
    task_logger.info("===============================")

    matching_files = []
    pattern_log = []
    for from_address, filename, part in email_details:
        if from_address in config:
            patterns_and_folders = config[from_address]
            found_match = False

            for item in patterns_and_folders:
                if item["pattern"] in filename:
                    matching_files.append((from_address, filename, item["destination"], part))
                    task_logger.info(f"- File: {filename}")
                    task_logger.info(f"    - Pattern: {item['pattern']}")
                    task_logger.info(f"    - Destination: {item['destination']}")

                    pattern_log.append({
                        'from_address': from_address,
                        'pattern': item["pattern"],
                        'filename': filename
                    })
                    found_match = True
            if not found_match:
                task_logger.info(f"No pattern match found for email: {from_address} and file: {filename}")
        else:
            # Log if the email address is not found in the config
            task_logger.info(f"Email address {from_address} not found in the configuration.")

    context['task_instance'].xcom_push(key='matching_files', value=matching_files)
    task_logger.info("===============================")

def write_files_to_destination(**context):
    import logging
    from base64 import b64decode
    import json
    import re

    task_logger = setup_task_logger('write_files_to_destination')
    config = context['task_instance'].xcom_pull(key='config', task_ids='load_config')
    year, week_number = get_current_week_info()
    matching_files = context['task_instance'].xcom_pull(key='matching_files', task_ids='check_patterns')

    task_logger.info("==================================================")
    task_logger.info("File Writing Summary:")
    task_logger.info("==================================================")
    task_logger.info(f"Total Files Written: {len(matching_files)}")

    move_log = []

    if not matching_files:
        task_logger.info("No matching files found.")
        return
    
    # Set up the Google Sheets API credentials
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/mnt/data/ftp/staffshare/public/stagiaire2/AIRFLOW/MATRIX/OUTLOOK/shaped-timing-432410-v4-8fb98bb23a3d.json', scope)
    client = gspread.authorize(creds)

    batch_updates = []

    for from_address, filename, destination, encoded_payload in matching_files:
        # Check if the filename contains Wxx or Sxx
        match = re.search(r'[WS](\d{1,2})|SEM(\d{1,2})', filename)
        if match:
            week_str = int(match.group(1) or match.group(2))  # Extract the week number part (xx)
            prefix = f"{year}52{week_str:02d}__"
        else:
            date_match = re.findall(r'(\d{4}[01]\d[0-3]\d)', filename)
            valid_date_found = False
            for date_str in date_match:
                try:
                    file_date = datetime.strptime(date_str, '%Y%m%d')
                    week_number = file_date.isocalendar()[1]  # Get the week number
                    if week_number == 1 and file_date.month == 12:
                        # If the date is in the last week of December, it belongs to the next year
                        prefix = f"{year + 1}52{week_number:02d}__"
                    else:
                        prefix = f"{year}52{week_number:02d}__"
                    valid_date_found = True
                    break  # Stop after finding the first valid date
                except ValueError:
                    continue
            if not valid_date_found:
                prefix = f"{year}52{week_number:02d}__"
        
        new_filename = prefix + filename
        destination_path = os.path.join(destination, new_filename)

        # Extract the week number from the prefix to determine the sheet name
        week_number_str = prefix[-4:-2]  # Extracts the '52xx' where xx is the week number
        sheet_name = f"24W{week_number_str}"  # Construct the sheet name
        #print(sheet_name) # Print
        # Open the spreadsheet
        try:
            spreadsheet = client.open("Matrix funnel 2024 - Check")
            sheet = spreadsheet.worksheet(sheet_name)

            all_values = sheet.get_all_values()
            col_g_values = [row[6].strip().lower() for row in all_values]  # Column G
            col_i_values = [row[8].strip().lower() for row in all_values]  # Column I

        except HttpError as e:
            task_logger.error(f"Failed to fetch worksheet {sheet_name}: {str(e)}")
            return  # Exit if the sheet can't be fetched
        except Exception as e:
            task_logger.error(f"Unexpected error fetching worksheet {sheet_name}: {str(e)}")
            return

        try:
            os.makedirs(destination, exist_ok=True)
            with open(destination_path, 'wb') as f:
                f.write(b64decode(encoded_payload))
            task_logger.info(f"File:")
            task_logger.info(f"  -> ORIGINAL FILENAME: {filename}")
            task_logger.info(f"  -> NEW FILENAME: {new_filename}")
            task_logger.info(f"  -> DESTINATION: {destination_path}")
            task_logger.info(f"  -> STATUS: Success")

            move_log.append({
                'filename': filename,
                'destination': destination_path,
                'status': 'Success',
                'from_address': from_address
            })

            # Normalize the email address
            normalized_from_address = from_address.strip().lower()

            for i, (g_value, i_value) in enumerate(zip(col_g_values, col_i_values)):
                if normalized_from_address in g_value:
                    for item in config[normalized_from_address]:
                        if item['pattern'].strip().lower() in i_value:
                            # Checkbox update in column C
                            batch_updates.append({
                                "updateCells": {
                                    "range": {
                                        "sheetId": sheet.id,
                                        "startRowIndex": i,
                                        "endRowIndex": i + 1,
                                        "startColumnIndex": 2,
                                        "endColumnIndex": 3
                                    },
                                    "rows": [
                                        {
                                            "values": [
                                                {
                                                    "userEnteredValue": {
                                                        "boolValue": True
                                                    }
                                                }
                                            ]
                                        }
                                    ],
                                    "fields": "userEnteredValue"
                                }
                            })

                            # Formatting update in column D
                            batch_updates.append({
                                "updateCells": {
                                    "range": {
                                        "sheetId": sheet.id,
                                        "startRowIndex": i,
                                        "endRowIndex": i + 1,
                                        "startColumnIndex": 3,
                                        "endColumnIndex": 4
                                    },
                                    "rows": [
                                        {
                                            "values": [
                                                {
                                                    "userEnteredFormat": {
                                                        "backgroundColor": {
                                                            "red": 1.0,
                                                            "green": 1.0,
                                                            "blue": 0.0
                                                        }
                                                    }
                                                }
                                            ]
                                        }
                                    ],
                                    "fields": "userEnteredFormat.backgroundColor"
                                }
                            })

                            task_logger.info(f"Prepared update for checkbox at C{i+1} and coloring D{i+1} yellow for provider {from_address}")


        except FileNotFoundError:
            task_logger.error(f"Path does not exist: {destination}")
            move_log.append({
                'filename': filename,
                'destination': destination_path,
                'status': 'Failed - Path does not exist',
                'from_address': from_address
            })
            
        except Exception as e:
            error_message = ''.join(traceback.format_exception(None, e, e.__traceback__))
            task_logger.error(f"Failed to save file {filename} to {destination}. Original error: {error_message}")
            move_log.append({
                'filename': filename,
                'destination': destination_path,
                'status': f'Failed - {error_message}',
                'from_address': from_address
            })
            
    # Perform all Google Sheets updates in one go
    if batch_updates:
        try:
            body = {'requests': batch_updates}
            spreadsheet.batch_update(body)
            task_logger.info("Batch update completed successfully.")
        except HttpError as e:
            task_logger.error(f"Failed to complete batch update due to quota limits: {str(e)}")
        except Exception as e:
            task_logger.error(f"Unexpected error during batch update: {str(e)}")


    task_logger.info("==================================================")

def summarize_run(**context):
    task_logger = setup_task_logger('Run Summary')
    task_logger.info("==================================================")
    task_logger.info("DAG Run Completed")
    task_logger.info("==================================================")
    task_logger.info("RUN SUMMARY:")
    
    email_details = context['task_instance'].xcom_pull(key='email_details', task_ids='receive_datafiles')
    matching_files = context['task_instance'].xcom_pull(key='matching_files', task_ids='check_patterns')

    # Emails processed is the count of distinct email addresses
    unique_email_addresses = set(from_address for from_address, filename, payload in email_details)
    task_logger.info(f"  - Emails Processed: {len(unique_email_addresses)}")

    # Attachments found is the total count of attachments processed
    total_attachments = len(email_details)
    task_logger.info(f"  - Attachments Found: {total_attachments}")

    # Files saved is the count of files written to destination
    task_logger.info(f"  - Files Saved: {len(matching_files)}")

    # Assuming no errors for now
    task_logger.info("  - Errors Encountered: None")
    task_logger.info("==================================================")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 28),
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=10),  # Delay between retries

}

with DAG(dag_id='Matrix_Outlook', default_args=default_args 
         #, schedule_interval=timedelta(hours=2)
         , schedule_interval=None
         , catchup=False) as dag:
    load_config_task = PythonOperator(
        task_id='load_config',
        python_callable=load_config,
        op_kwargs={'file_path': '/mnt/data/ftp/staffshare/public/stagiaire2/AIRFLOW/MATRIX/OUTLOOK/config_outlook.json'},
        provide_context=True
    )
    
    receive_datafiles_task = PythonOperator(
        task_id='receive_datafiles',
        python_callable=receive_datafiles,
        provide_context=True
    )
    
    check_patterns_task = PythonOperator(
        task_id='check_patterns',
        python_callable=check_patterns,
        provide_context=True
    )
    
    write_files_to_destination_task = PythonOperator(
        task_id='write_files_to_destination',
        python_callable=write_files_to_destination,
        provide_context=True
    )
    
    summarize_run_task = PythonOperator(
    task_id='summarize_run',
    python_callable=summarize_run,
    provide_context=True
    )

    load_config_task >> receive_datafiles_task >> check_patterns_task >> write_files_to_destination_task >> summarize_run_task
