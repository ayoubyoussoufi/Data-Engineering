import imaplib
import email
import os
import json
from email.header import decode_header
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from imapclient import IMAPClient
import zipfile
import shutil
import pandas as pd
import gzip
import tarfile
import re

# IMAP server credentials
IMAP_SERVER = 'imap.ionos.fr'
IMAP_USER = 'victoria@aqoa.fr'
IMAP_PASS = 'V@room22'


def extract_week_number(filename):
    # First, try to match patterns like Sxx or Wxx
    match = re.search(r'[WS](\d{1,2})', filename)
    if match:
        return int(match.group(1))

    # Second, try to find patterns like yyyyxx_yyyyxx (ensure no single number follows)
    match = re.search(r'(\d{4})(\d{2})_(\d{4})(\d{2})', filename)
    if match and match.group(1) == match.group(3):
        return int(match.group(4))

    # Third, try to match yyyyxx (e.g., 202433), ensuring it's not followed by a digit
    match = re.search(r'(\d{4})(\d{2})(?!\d)', filename)
    if not match:
        match = re.search(r'(\d{4})(\d{2})(?!\d)', filename)
        if match:
            return int(match.group(2))

    # Fourth, try to match dates in DD.MM au DD.MM format
    date_match = re.search(r'(\d{2})\.(\d{2})\sau\s(\d{2})\.(\d{2})', filename)
    if date_match:
        day1, month1 = int(date_match.group(1)), int(date_match.group(2))
        day2, month2 = int(date_match.group(3)), int(date_match.group(4))
        try:
            date_str = f"{datetime.now().year}-{month2:02d}-{day2:02d}"
            file_date = datetime.strptime(date_str, '%Y-%m-%d')
            week_number = file_date.isocalendar()[1]
            return week_number
        except ValueError:
            None

    # Fifth, try to match MM-DD-YY format
    date_match = re.search(r'(\d{2})-(\d{2})-(\d{2})', filename)
    if date_match:
        month, day, year_suffix = int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))
        # Assume years in the 2000s for two-digit years (this could be adjusted as needed)
        year = 2000 + year_suffix
        try:
            date_str = f"{year}-{month:02d}-{day:02d}"
            file_date = datetime.strptime(date_str, '%Y-%m-%d')
            week_number = file_date.isocalendar()[1] - 1
            return week_number
        except ValueError:
            None

    # Sixth, try to match dates in YYYY-MM-DD format
    date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})(?=[^\d]|$)', filename)
    if date_match:
        year, month, day = int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))
        try:
            date_str = f"{year}-{month:02d}-{day:02d}"
            file_date = datetime.strptime(date_str, '%Y-%m-%d')
            week_number = file_date.isocalendar()[1]
            return week_number
        except ValueError:
            None

    return None


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
    from datetime import datetime
    import re

    def sanitize_filename(filename):
        """Sanitize the filename to remove or replace invalid characters."""
        return "".join(c if c.isalnum() or c in "._- " else "_" for c in filename)

    def setup_task_logger(name):
        """Setup logger for Airflow tasks"""
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

    task_logger = setup_task_logger('Receiving data files')
    config = context['task_instance'].xcom_pull(key='config', task_ids='load_config')
    task_logger.info("EMAIL PROCESSING SUMMARY:")
    task_logger.info("--------------------------------------------------")

    try:
        with IMAPClient(IMAP_SERVER) as mail:
            mail.login(IMAP_USER, IMAP_PASS)

            folder_name = 'INBOX/DST/1. Réception'
            mail.select_folder(folder_name)

            messages = mail.search('UNSEEN')
            task_logger.info(f"Total Unseen Emails Found: {len(messages)}")
            task_logger.info("--------------------------------------------------")

            email_details = []
            email_log = {}
            year, week_number = get_current_week_info()
            production_week = week_number

            for email_id in messages:
                msg_data = mail.fetch([email_id], ['RFC822'])
                msg = email.message_from_bytes(msg_data[email_id][b'RFC822'])

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

                            # Extract week number from filename
                            file_week = extract_week_number(filename)

                            if file_week and file_week > production_week:
                                # Mark email as unseen again since the file is too recent
                                mail.remove_flags([email_id], ['\\Seen'], silent=False)
                                task_logger.info(
                                    f"  -> Skipping file: {filename}, week number {file_week} is too recent P.S :  Week of Production is {production_week}.")
                            else:
                                payload = b64encode(part.get_payload(decode=True)).decode('utf-8')
                                email_details.append((from_address, filename, payload))
                                attachments.append(filename)

                if not attachments:
                    task_logger.info("  -> No valid attachments found.")

                email_log[from_address] = attachments
                task_logger.info("--------------------------------------------------")
                task_logger.info("")  # Add empty line between emails

                # Mark the email as seen only if valid files were found
                if attachments:
                    mail.set_flags([email_id], ['\\Seen'])

            context['task_instance'].xcom_push(key='email_details', value=email_details)
            task_logger.info(f"Email details: {json.dumps(email_log, indent=2)}")
            task_logger.info("==================================================")
            task_logger.info("")  # Add empty line at the end of the section

    except Exception as e:
        task_logger.error(f"Error processing emails: {e}")


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
            found_match = False  # Flag to track if a pattern is matched

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

            # If no match was found for the email address
            if not found_match:
                task_logger.info(f"No pattern match found for email: {from_address} and file: {filename}")
        else:
            # Log if the email address is not found in the config
            task_logger.info(f"Email address {from_address} not found in the configuration.")

    context['task_instance'].xcom_push(key='matching_files', value=matching_files)
    task_logger.info("===============================")


def write_files_to_destination(**context):
    from base64 import b64decode
    import re

    task_logger = setup_task_logger('write_files_to_destination')
    config = context['task_instance'].xcom_pull(key='config', task_ids='load_config')
    year, week_number = get_current_week_info()
    print(f" Semaine de production : {week_number}")
    matching_files = context['task_instance'].xcom_pull(key='matching_files', task_ids='check_patterns')

    task_logger.info("==================================================")
    task_logger.info("File Writing Summary:")
    task_logger.info("==================================================")
    task_logger.info(f"Total Files Written: {len(matching_files)}")

    def process_file(destination_path, prefix):
        filename = os.path.basename(destination_path)  # Get the filename from the path
        output_dir = os.path.dirname(destination_path)  # Get the directory path
        new_filename = prefix + filename
        temp_dir = '/mnt/data/ftp/staffshare/public/stagiaire2/AIRFLOW/DST/OUTLOOK/TEMP_Holder_zipfiles'

        print(f"Processing file: {filename}")

        # Handle .zip files
        import shutil
        from shutil import copyfile

        if filename.endswith('.zip'):
            try:
                print("before")
                # Extract in the temporary directory
                with zipfile.ZipFile(destination_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                print("extracted the zip into temporary directory")

                # List extracted items
                extracted_items = os.listdir(temp_dir)

                if extracted_items:
                    for item in extracted_items:
                        item_path = os.path.join(temp_dir, item)

                        if os.path.isfile(item_path):  # Handle files directly extracted into temp_dir
                            print(f"Extracted file: {item}")

                            # Prefix the file and prepare the destination path
                            new_extracted_filename = prefix + item
                            final_destination_path = os.path.join(output_dir, new_extracted_filename)

                            # Copy the file to the destination
                            shutil.copyfile(item_path, final_destination_path)
                            print(f"Copied {item} to {final_destination_path}")

                        elif os.path.isdir(item_path):  # Handle subfolders
                            print(f"Extracted folder path: {item_path}")

                            # Move and prefix files inside the extracted folder
                            for extracted_file in os.listdir(item_path):
                                extracted_file_path = os.path.join(item_path, extracted_file)

                                # Prefix the file and prepare the destination path
                                new_extracted_filename = prefix + extracted_file
                                final_destination_path = os.path.join(output_dir, new_extracted_filename)

                                if os.path.isfile(extracted_file_path):
                                    print(f"Copying file: {extracted_file}")
                                    # Copy the file to the destination
                                    shutil.copyfile(extracted_file_path, final_destination_path)
                                    print(f"Copied {extracted_file} to {final_destination_path}")
                                else:
                                    print(f"Skipped {extracted_file} as it is not a file")

                else:
                    print(f"Error: No items were found after extraction.")
                    return False

                # Remove the original zip file after processing
                os.remove(destination_path)
                print("Removing the zip")
                task_logger.info(f"Zip file {filename} handled and extracted successfully.")

                # Clean up the temp directory after moving the files
                try:
                    for temp_file in os.listdir(temp_dir):
                        file_path = os.path.join(temp_dir, temp_file)
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                            os.remove(file_path)  # Remove the file
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)  # Remove the directory and its contents
                    print(f"Temporary directory {temp_dir} emptied.")
                except Exception as e:
                    task_logger.error(f"Failed to empty the temporary directory {temp_dir}: {e}")

                return True

            except Exception as e:
                task_logger.info(f"Failed to handle zip file {filename}: {e}")
                return False

        # Handle .gz and .tar.gz files
        elif filename.endswith('.gz'):
            try:
                if filename.endswith('.tar.gz'):
                    # Handle .tar.gz files using tarfile module
                    with tarfile.open(destination_path, 'r:gz') as tar_ref:
                        tar_ref.extractall(output_dir)
                else:
                    # Handle simple .gz files using gzip module
                    extracted_filename = filename.replace('.gz', '')
                    extracted_file_path = os.path.join(output_dir, extracted_filename)
                    with gzip.open(destination_path, 'rb') as gz_ref:
                        with open(extracted_file_path, 'wb') as extracted_file:
                            shutil.copyfileobj(gz_ref, extracted_file)
                    new_extracted_filename = extracted_filename  # + prefix in case of some day the newfilename doesnt have a prefix
                    shutil.move(extracted_file_path, os.path.join(output_dir, new_extracted_filename))

                # Remove the original gz file
                os.remove(destination_path)
                task_logger.info(f"GZ file {filename} handled and extracted successfully.")
            except Exception as e:
                task_logger.info(f"Failed to handle GZ file {filename}: {e}")
                return False

        # Handle .dat files
        elif filename.endswith('.dat'):
            try:
                # Remove the '.dat' part from the filename and rename it
                csv_filename = filename.replace('.dat', '').strip()
                csv_destination_path = os.path.join(output_dir, csv_filename)

                # Rename the file by moving it
                shutil.move(destination_path, csv_destination_path)
                task_logger.info(f".dat file {filename} renamed to {csv_filename} successfully.")
            except Exception as e:
                task_logger.info(f"Failed to rename .dat file {filename}: {e}")
                return False

        # For non-zip, non-gz, and non-dat files, do nothing
        else:
            task_logger.info(f"File {filename} does not require special handling.")

        return True

    move_log = []

    if not matching_files:
        task_logger.info("No matching files found.")
        return

    for from_address, filename, destination, encoded_payload in matching_files:
        sanitized_filename = sanitize_filename(filename)  # Sanitize filename here
        # Check if the filename contains Wxx or Sxx
        week_number = extract_week_number(sanitized_filename)
        prefix = None
        if week_number is not None:
            prefix = f"{year}52{week_number:02d}__"
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
                year, week_number = get_current_week_info()
                print(week_number)

                prefix = f"{year}52{week_number:02d}__"

        new_filename = prefix + filename
        destination_path = os.path.join(destination, new_filename)

        try:
            os.makedirs(destination, exist_ok=True)
            with open(destination_path, 'wb') as f:
                f.write(b64decode(encoded_payload))

            process_file(destination_path, prefix)
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
        except FileNotFoundError:
            task_logger.error(f"Path does not exist: {destination}")
            move_log.append({
                'filename': filename,
                'destination': destination_path,
                'status': 'Failed - Path does not exist',
                'from_address': from_address
            })
        except Exception as e:
            task_logger.error(f"Failed to save file {filename} to {destination}: {e}")
            move_log.append({
                'filename': filename,
                'destination': destination_path,
                'status': f'Failed - {e}',
                'from_address': from_address
            })

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

with DAG(dag_id='DST_Outlook', default_args=default_args
         # , schedule_interval=timedelta(hours=2)
        , schedule_interval=None
        , catchup=False) as dag:
    load_config_task = PythonOperator(
        task_id='load_config',
        python_callable=load_config,
        op_kwargs={'file_path': '/mnt/data/ftp/staffshare/public/stagiaire2/AIRFLOW/DST/OUTLOOK/config_outlook.json'},
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
