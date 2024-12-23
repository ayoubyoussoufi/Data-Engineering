import pendulum
from airflow import DAG
from airflow.decorators import task, dag, task_group
from airflow.operators.empty import EmptyOperator
from random import randint
from pendulum import datetime
import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import logging
from airflow.utils.trigger_rule import TriggerRule
import re
import hashlib
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from airflow import DAG
from datetime import datetime



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

def get_current_week_info():
    today = datetime.today()
    week_start = today - timedelta(days=today.weekday())
    previous_week_start = week_start - timedelta(weeks=1)
    year, week_number, _ = previous_week_start.isocalendar()
    return year, week_number

def sanitize_filename(filename):
    # Remove non-ASCII characters
    return re.sub(r'[^\x00-\x7F]+', '', filename)

def calculate_checksum(file_path):
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()
@dag(
    dag_id="Matrix_Filer",
    start_date=datetime(2024, 7, 8),
    #schedule_interval='0 12,17 * * *',
    schedule_interval=None,
    catchup=False,
)
def task_group_mapping_example():

    ####################  LIST OF PROVIDERS #######################################
    @task(task_id="charger_configuration")
    def load_config(file_path):
        task_logger = setup_task_logger("Configuration Loader")
        task_logger.info("==================================================")
        task_logger.info(f"DAG Run Initiated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        task_logger.info("==================================================")
        providers_info = []


        try:
            task_logger.info("CONFIGURATION STATUS:")
            task_logger.info(f"  - Source: {file_path}")
            with open(file_path, 'r') as file:
                config = json.load(file)
            task_logger.info("  - Status: Loaded Successfully")

            for provider in config["providers"]:
                provider_info = {
                    "name": provider["name"],
                    "default_prefix": provider["default_prefix"],
                    "patterns": provider["patterns"],
                    "checksum_activated": provider["checksum_activated"],
                    "requires_manual_config": provider["requires_manual_config"],
                    "paths": provider["paths"]
                }
                providers_info.append(provider_info)
        except Exception as e:
            task_logger.error(f"  - Error loading configuration: {e}")
            raise
        task_logger.info("==================================================")

        return providers_info

    ###############################################################################################


    ##########################  TASK GROUP OF CHECK AND MOVE FILES#####################################################################

    @task_group(group_id ="groupe_verification_et_deplacement")
    def tg2(provider_info):
        @task(task_id = "verifier_nouveaux_fichiers")
        def check_for_new_files(provider_info):
            provider_name = provider_info["name"]
            checksum_activated = provider_info["checksum_activated"]
            task_logger = setup_task_logger(f'Check for new files {provider_name}')

            task_logger.info("==================================================")
            task_logger.info(f"CHECKING FOR NEW FILES FOR {provider_name.upper()}")
            task_logger.info("==================================================")
            task_logger.info("Received configuration")
            task_logger.info("--------------------------------------------------")

            processed_files_csv = f'/mnt/data/ftp/staffshare/public/stagiaire2/AIRFLOW/MATRIX/FILER/saved_processed_files/processed_files_{provider_name}.csv'
            processed_files = {}

            if os.path.exists(processed_files_csv):
                with open(processed_files_csv, 'r') as f:
                    for line in f:
                        parts = line.strip().split(',')
                        if len(parts) >= 1 and parts[0].strip():  # Ensure it's not an empty or malformed line
                            filename = parts[0].strip()
                            checksum = parts[1].strip() if len(parts) > 1 else None
                            processed_files[filename] = checksum
            else:
                task_logger.error(f"Processed files CSV does not exist: {processed_files_csv}")
                raise FileNotFoundError(f"Processed files CSV does not exist: {processed_files_csv}")

            new_files = []
            for path_info in provider_info['paths']:
                source_path = path_info['source_path']
                destination_folders = path_info['destination_folders']

                task_logger.info(f"Checking path: {source_path}")

                if os.path.exists(source_path):
                    for filename in os.listdir(source_path):
                        sanitized_filename = sanitize_filename(filename)
                        full_path = os.path.join(source_path, filename)


                        # Determine if the file matches any pattern or if patterns list is empty
                        pattern_matched = any(pattern_info["pattern"] in sanitized_filename for pattern_info in provider_info["patterns"]) or not provider_info["patterns"]

                        if pattern_matched and sanitized_filename not in processed_files:
                            if checksum_activated:
                                file_checksum = calculate_checksum(full_path)
                                if sanitized_filename not in processed_files or processed_files[sanitized_filename] != file_checksum:
                                    new_files.append((full_path, destination_folders))
                                    task_logger.info(f"  -> Found new file: {sanitized_filename} with checksum {file_checksum}")
                                else:
                                    task_logger.info(f"  -> Skipping file (checksum matches): {sanitized_filename}")
                            else:
                                if sanitized_filename not in processed_files:
                                    new_files.append((full_path, destination_folders))
                                    task_logger.info(f"  -> Found new file: {sanitized_filename}")
                        else:
                            if not pattern_matched:
                                task_logger.info(f"  -> Skipping file (no matching pattern): {sanitized_filename}")

                else:
                    task_logger.error(f"Source path does not exist: {source_path}")

            if not new_files:
                task_logger.info("  -> No new files found.")
            task_logger.info("--------------------------------------------------")

            task_logger.info("SUMMARY OF NEW FILES:")
            task_logger.info(f"{json.dumps(new_files, indent=2)}")
            task_logger.info("==================================================")

            return new_files

        @task(task_id="deplacer_fichiers_dossier")
        def move_files_to_destination(provider_info, new_files):
            import logging
            import json
            import re
            from shutil import copyfile
            provider_name = provider_info["name"]
            task_logger = setup_task_logger(f'Move files to destination {provider_name}')

            if not new_files:
                task_logger.info(f"No new files found for provider {provider_name}")
                return {"provider_name": provider_name, "move_log": []}

            year, current_week_number = get_current_week_info()
            default_prefix = provider_info["default_prefix"]

            task_logger.info("==================================================")
            task_logger.info(f"MOVING FILES TO DESTINATIONS FOR {provider_name.upper()}")
            task_logger.info("==================================================")
            task_logger.info(f"Total Files to Move: {len(new_files)}")

            move_log = []
            processed_files_csv = f'/mnt/data/ftp/staffshare/public/stagiaire2/AIRFLOW/MATRIX/FILER/saved_processed_files/processed_files_{provider_name}.csv'

            def extract_week_number(filename):
                # First, look for the 'W' or 'S' followed by a number
                match = re.search(r'[WS](\d{1,2})', filename)
                if match:
                    return int(match.group(1))

                # If not found, look for 'Sales' followed by a number (with optional leading zero)
                match = re.search(r'Sales\s(\d{1,2})', filename)
                if match:
                    return int(match.group(1))

                return None


            for full_path, destination_folders in new_files:
                filename = os.path.basename(full_path)
                sanitized_filename = sanitize_filename(filename)  # Sanitize filename here

                # Determine the week number from the filename
                week_number = extract_week_number(sanitized_filename)
                prefix = None

                if week_number is not None:
                    prefix = f"{year}52{week_number:02d}__"
                else:
                    pattern = r'((?:\d{4}[-]?[01]\d[-]?[0-3]\d)|(?:\d{2}[-]?[01]\d[-]?(?:19|20)\d{2}))'
                    date_match = re.findall(pattern, sanitized_filename)
                    # Function to extract and calculate week number
                    def get_week_number(date_str):
                        try:
                            if '-' in date_str:
                                parts = date_str.split('-')
                            else:
                                if len(date_str) == 8:
                                    if date_str[:4].isdigit() and int(date_str[:4]) >= 2023:
                                        parts = [date_str[:4], date_str[4:6], date_str[6:]]
                                    else:
                                        parts = [date_str[:2], date_str[2:4], date_str[4:]]
                                else:
                                    parts = [date_str[:2], date_str[2:4], date_str[4:]]

                            if len(parts[0]) == 4:
                                # YYYYMMDD or YYYY-MM-DD
                                formatted_date_str = f"{parts[0]}-{parts[1]}-{parts[2]}"
                                file_date = datetime.strptime(formatted_date_str, '%Y-%m-%d')
                            else:
                                # DDMMYYYY or DD-MM-YYYY
                                formatted_date_str = f"{parts[2]}-{parts[1]}-{parts[0]}"
                                file_date = datetime.strptime(formatted_date_str, '%Y-%m-%d')

                            week_number = file_date.isocalendar()[1]
                            return week_number, file_date
                        except ValueError:
                            # Step 2: If Step 1 fails, attempt to extract dates using regex
                            try:
                                # Updated regex to be more flexible and ensure proper matching
                                # This pattern looks for exactly 8 consecutive digits
                                matches = re.findall(r'\d{8}', date_str)
                                if matches:
                                    for match in matches:
                                        try:
                                            # Attempt to parse the matched string as DDMMYYYY
                                            file_date = datetime.strptime(match, '%d%m%Y')
                                            week_number = file_date.isocalendar()[1]
                                            return week_number, file_date
                                        except ValueError:
                                            # If parsing fails, continue to the next match
                                            continue
                            except Exception:
                                # Catch any unexpected exceptions
                                pass

                        # If all parsing attempts fail, return None
                        return None, None


                    valid_date_found = False

                    for date_str in date_match:
                        try:
                            week_number, file_date = get_week_number(date_str)
                            if week_number == 1 and file_date.month == 12:
                                # If the date is in the last week of December, it belongs to the next year
                                prefix = f"{year + 1}52{week_number:02d}__"
                            else:
                                prefix = f"{year}52{week_number:02d}__"
                            valid_date_found = True
                            break  # Stop after finding the first valid date
                        except ValueError:
                            continue

                # Ensure the derived prefix is always used when the default prefix is "Weekly"
                if default_prefix == "Weekly":
                    if prefix is None:
                        week_number = f"{current_week_number:02d}"  # Use the current week number if none is found in the filename or date
                        prefix = f"{year}52{week_number}__"
                elif default_prefix == "Monthly":
                    if prefix is None:
                        prefix = f"_MONTHLY_"
                elif default_prefix == "Undefined":
                    if prefix is None:
                        prefix = ""
                        week_number = f"{current_week_number:02d}"
                        for pattern_info in provider_info["patterns"]:
                            if pattern_info["pattern"] in sanitized_filename:
                                if pattern_info["prefix"] == "Weekly":
                                    prefix = f"{year}52{week_number}__"
                                elif pattern_info["prefix"] == "Monthly":
                                    prefix = f"_MONTHLY_"
                                break

                new_filename = prefix + sanitized_filename
                task_logger.info(f"Processing file: {full_path}")

                for destination_folder in destination_folders:
                    destination_path = os.path.join(destination_folder, new_filename)

                    try:
                        os.makedirs(destination_folder, exist_ok=True)
                        copyfile(full_path, destination_path)
                        task_logger.info(f"  -> Copying to: {destination_path}")
                        task_logger.info(f"  -> Status: Success")
                        move_log.append({
                            'filename': sanitized_filename,  # Log sanitized filename
                            'destination': destination_path,
                            'status': 'Success'
                        })
                    except FileNotFoundError:
                        task_logger.error(f"  -> Path does not exist: {destination_folder}")
                        move_log.append({
                            'filename': sanitized_filename,  # Log sanitized filename
                            'destination': destination_path,
                            'status': 'Failed - Path does not exist'
                        })
                    except Exception as e:
                        task_logger.error(f"  -> Failed to copy file {sanitized_filename} to {destination_folder}: {e}")
                        move_log.append({
                            'filename': sanitized_filename,  # Log sanitized filename
                            'destination': destination_path,
                            'status': f'Failed - {e}'
                        })

                # Log the sanitized filename and its checksum if checksum is activated
                if provider_info["checksum_activated"]:
                    file_checksum = calculate_checksum(full_path)
                    with open(processed_files_csv, 'a') as f:
                        f.write(f"{sanitized_filename},{file_checksum}\n")
                else:
                    with open(processed_files_csv, 'a') as f:
                        f.write(sanitized_filename + '\n')  # Write sanitized filename

                task_logger.info("--------------------------------------------------")

            task_logger.info("FILE MOVE DETAILS:")
            task_logger.info(f"{json.dumps(move_log, indent=2)}")
            task_logger.info("==================================================")

            return {"provider_name": provider_name, "move_log": move_log}

        @task(task_id='update_google_sheet')
        def update_google_sheet(provider_info, move_log):
            # Extract the provider name from provider_info
            provider_name = move_log.get('provider_name')
            print(f"provider_name is : {provider_name}")

            # Extract the actual move log list
            move_log_list = move_log.get('move_log', [])
            print(f"move_log_list is : {move_log_list}")

            # Define the scope and credentials for Google Sheets API
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name('/mnt/data/ftp/staffshare/public/stagiaire2/AIRFLOW/MATRIX/OUTLOOK/shaped-timing-432410-v4-8fb98bb23a3d.json', scope)
            client = gspread.authorize(creds)

            if move_log_list:
                # Get the previous week's information
                year, previous_week_number = get_current_week_info()
                previous_week_number_str = f"{previous_week_number:02d}"  # Format previous week number as two digits

                # Open the Google Sheet by its name
                spreadsheet = client.open("Matrix funnel 2024 - Check")

                # Iterate over all moved files in the log
                for log_entry in move_log_list:
                    print(f"log_entry is for 1 : {log_entry}")
                    if isinstance(log_entry, dict):  # Ensure log_entry is a dictionary
                        filename = log_entry.get('destination').split('/')[-1]  # Extract the filename from the destination path
                        print(f"Processing filename: {filename}")  # Debug print

                        # Determine the sheet name based on the filename's prefix
                        if filename.startswith('_MONTHLY_'):
                            # Use the previous week sheet
                            sheet_name = f"24W{previous_week_number_str}"
                            search_pattern = f"{provider_name} - mensuel"
                            print(f"Monthly file detected. Using sheet: {sheet_name}")  # Debug print
                        else:
                            # Extract week number from the filename's prefix
                            week_number_match = re.search(r'52(\d{2})__', filename)
                            if week_number_match:
                                week_number_str = week_number_match.group(1)
                                sheet_name = f"24W{week_number_str}"
                                print(f"Week number found: {week_number_str}. Using sheet: {sheet_name}")  # Debug print
                            else:
                                # Default to the previous week if no specific week number is found
                                sheet_name = f"24W{previous_week_number_str}"
                                print(f"No week number found. Using default sheet: {sheet_name}")  # Debug print

                            search_pattern = provider_name

                        # Select the appropriate sheet based on the derived sheet name
                        try:
                            sheet = spreadsheet.worksheet(sheet_name)
                            print(f"Successfully opened sheet: {sheet_name}")  # Debug print
                        except Exception as e:
                            print(f"Error opening sheet {sheet_name}: {e}")
                            continue  # Skip this entry if the sheet cannot be opened

                        # Iterate over all cells in column H to find the correct match
                        cell_list = sheet.col_values(8)  # Column H is the 8th column
                        print(f"cell_list: {cell_list}") # Debug print
                        for i, cell_value in enumerate(cell_list):
                            if search_pattern in cell_value:
                                # Row number is i+1 because lists are 0-indexed but sheet rows are 1-indexed
                                checkbox_cell = f'C{i+1}'
                                color_cell = f'D{i+1}'
                                print(f"Updating {checkbox_cell}")  # Debug print
                                sheet.update_acell(checkbox_cell, 'TRUE')
                                                # Color cell D18 (D17 in the script) yellow
                                cell_format = {
                                    "backgroundColor": {
                                        "red": 1.0,
                                        "green": 1.0,
                                        "blue": 0.0
                                    }
                                }
                                sheet.format(color_cell, cell_format)
                                print(f"Updated checkbox for provider {provider_name} in row {i+1} for file {filename}")


                                break  # Stop once the correct row is found for this file

        new_files = check_for_new_files(provider_info)
        move_log = move_files_to_destination(provider_info, new_files)
        update_google_sheet(provider_info, move_log)



    ###############################################################################################





    ############################## RUN SUMMARIZE #################################################################



    from airflow.models import TaskInstance
    from airflow.utils.state import State

    @task(task_id='resume_execution')
    def run_summary(**context):
        task_logger = setup_task_logger('Run Summary')
        task_logger.info("==================================================")
        task_logger.info("DAG RUN COMPLETED")
        task_logger.info("==================================================")
        task_logger.info("RUN SUMMARY:")

        total_files_processed = 0
        total_files_moved = 0

        # Retrieve the providers from the XCom
        providers_info = context['ti'].xcom_pull(task_ids='charger_configuration', key='return_value')
        providers = [provider["name"] for provider in providers_info]

        for provider_index, provider in enumerate(providers):
            check_task_id = f'groupe_verification_et_deplacement.verifier_nouveaux_fichiers'
            move_task_id = f'groupe_verification_et_deplacement.deplacer_fichiers_dossier'

            # Pull the results from XCom
            new_files = context['ti'].xcom_pull(task_ids=check_task_id, key='return_value')[provider_index]
            move_logs = context['ti'].xcom_pull(task_ids=move_task_id, key='return_value')[provider_index]

            new_files_for_provider = new_files if new_files else []
            move_log_for_provider = move_logs["move_log"] if move_logs else []

            task_logger.info(f"{provider}:")

            # Print New Files
            if new_files_for_provider:
                task_logger.info("  New Files:")
                for file, _ in new_files_for_provider:
                    try:
                        task_logger.info(f"    - {file}")
                    except UnicodeEncodeError:
                        task_logger.info(f"    - {file.encode('utf-8', 'ignore').decode('utf-8')}")

            else:
                task_logger.info("  New Files: None")

            # Print Move Logs
            if move_log_for_provider:
                task_logger.info("  Move Logs:")
                for log in move_log_for_provider:
                    try:
                        task_logger.info(f"    - {log['filename']} ->")
                        if isinstance(log['destination'], str):
                            destinations = [log['destination']]
                        else:
                            destinations = log['destination']
                        for destination in destinations:
                            task_logger.info(f"      {destination}")
                    except UnicodeEncodeError:
                        task_logger.info(f"    - {log['filename'].encode('utf-8', 'ignore').decode('utf-8')} ->")
                        if isinstance(log['destination'], str):
                            destinations = [log['destination']]
                        else:
                            destinations = log['destination']
                        for destination in destinations:
                            task_logger.info(f"      {destination.encode('utf-8', 'ignore').decode('utf-8')}")
            else:
                task_logger.info("  Move Logs: None")

            total_files_processed += len(new_files_for_provider)
            total_files_moved += len([log for log in move_log_for_provider if log['status'] == 'Success'])

            task_logger.info("-----------------------------------------------------------")

        task_logger.info(f"  - Total Files Processed: {total_files_processed}")
        task_logger.info(f"  - Total Files Moved: {total_files_moved}")
        task_logger.info("==================================================")

    ##########################################################################################################

    # Dummy operator at the start of the DAG
    start = EmptyOperator(task_id='start')

    ################################## PROVIDERS LIST #############################################################
    file_path = '/mnt/data/ftp/staffshare/public/stagiaire2/AIRFLOW/MATRIX/FILER/CONFIG/config_filer.json'
    providers_info = load_config(file_path)

    ##############################RUN THE MAPPING ON EACH PROVIDER #################################################################
    tg2_object = tg2.expand(provider_info=providers_info)

    ######################## THE DAG #######################################################################
    start >> providers_info >> tg2_object >> run_summary()

task_group_mapping_example()
