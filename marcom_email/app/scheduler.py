import os
import argparse
import pandas as pd
import logging
from datetime import datetime, timedelta
from app.email_service import (
    load_excel_data,
    send_email,
    generate_email_content,
    update_excel
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_email_template(email_number):
    """
    Load the email template file based on the given email number.
    
    Args:
        email_number (int): The number representing the email to send (1, 2, or 3).

    Returns:
        str: The contents of the email template if found, or None if an error occurs.
    """
    template_map = {
        1: 'email_template_1.html',
        2: 'email_template_2.html',
        3: 'email_template_3.html'
    }
    
    template_name = template_map.get(email_number)
    if not template_name:
        logging.error(f"No template available for email number: {email_number}")
        return None

    template_path = os.path.join('/mnt/c/Users/eaeop/Desktop/email_campaign/app/templates', template_name)
    try:
        with open(template_path, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        logging.error(f"Template file not found: {template_name}")
        return None

def schedule_emails():
    """
    Schedule and send daily emails based on the data in an Excel file.
    """
    file_path = '/mnt/c/Users/eaeop/Desktop/email_campaign/emails.xlsx'   # Path to change
    pdf_attachment = '/mnt/c/Users/eaeop/Desktop/email_campaign/documents/Brochure.pdf'  # Path to PDF for email 1

    # Load data from the Excel file
    df = load_excel_data(file_path)

    if df is not None:
        overall_success = True  # Start by assuming success
        
        for index, row in df.iterrows():
            to_email = row['Email']
            last_sent_date_1 = row['Email 1 Sent']  # Get the last sent date for Email 1
            last_sent_date_2 = row['Email 2 Sent']  # Get the last sent date for Email 2
            last_sent_date_3 = row['Email 3 Sent']  # Get the last sent date for Email 3

            logging.info(f"Last sent dates for {to_email}: Email 1: {last_sent_date_1}, Email 2: {last_sent_date_2}, Email 3: {last_sent_date_3}")

            # Check for Email 1: If not sent or more than 24 hours ago, send it
            if pd.isna(last_sent_date_1) or pd.isnull(last_sent_date_1):
                logging.info(f"No previous Email 1 sent for {to_email}. Ready to send Email 1.")
                subject, body = generate_email_content(row, 1)
                email_sent = send_email(to_email, subject, body, pdf_attachment)

                if email_sent:
                    current_date_str = datetime.now().strftime('%Y-%m-%d')

                    # Ensure the column is of datetime type before updating it
                    df[f'Email 1 Sent'] = pd.to_datetime(df[f'Email 1 Sent'], errors='coerce')

                    # Now assign the current date as a datetime object
                    df.at[index, f'Email 1 Sent'] = pd.to_datetime(current_date_str)

                    excel_update_successful = update_excel(df, file_path, index, 1, current_date_str)
                    if not excel_update_successful:
                        logging.error(f"Error updating Excel after sending Email 1 to {to_email}.")
                        overall_success = False
                else:
                    logging.error(f"Failed to send Email 1 to {to_email}.")
                    overall_success = False

            # Check for Email 2: If Email 1 is sent and more than 24 hours ago, send Email 2
            else:
                last_sent_date_1 = pd.to_datetime(last_sent_date_1, errors='coerce')
                if pd.isna(last_sent_date_1):
                    logging.info(f"Email 1 date for {to_email} is invalid. Skipping Email 2.")
                else:
                    # Check if more than 24 hours have passed since Email 1 was sent
                    if (last_sent_date_1 + timedelta(days=1)) <= datetime.now():
                        if pd.isna(last_sent_date_2) or pd.isnull(last_sent_date_2):
                            logging.info(f"More than 24 hours since Email 1 for {to_email}. Ready to send Email 2.")
                            
                            subject, body = generate_email_content(row, 2)
                            email_sent = send_email(to_email, subject, body)

                            if email_sent:
                                current_date_str = datetime.now().strftime('%Y-%m-%d')

                                # Ensure the column is of datetime type before updating it
                                df[f'Email 2 Sent'] = pd.to_datetime(df[f'Email 2 Sent'], errors='coerce')

                                # Now assign the current date as a datetime object
                                df.at[index, f'Email 2 Sent'] = pd.to_datetime(current_date_str)

                                excel_update_successful = update_excel(df, file_path, index, 2, current_date_str)
                                if not excel_update_successful:
                                    logging.error(f"Error updating Excel after sending Email 2 to {to_email}.")
                                    overall_success = False
                            else:
                                logging.error(f"Failed to send Email 2 to {to_email}.")
                                overall_success = False
                        else:
                            logging.info(f"Email 2 already sent to {to_email}. Skipping Email 2.")
                    else:
                        logging.info(f"Less than 24 hours since Email 1 sent to {to_email}. Skipping Email 2.")

            # Check for Email 3: If Email 2 is sent and more than 24 hours ago, send Email 3
            last_sent_date_2 = pd.to_datetime(last_sent_date_2, errors='coerce')
            if pd.isna(last_sent_date_2):
                logging.info(f"Email 2 date for {to_email} is invalid. Skipping Email 3.")
            else:
                if (last_sent_date_2 + timedelta(days=1)) <= datetime.now() and (pd.isna(last_sent_date_3) or pd.isnull(last_sent_date_3)):
                    logging.info(f"More than 24 hours since Email 2 for {to_email}. Ready to send Email 3.")
                    
                    subject, body = generate_email_content(row, 3)
                    email_sent = send_email(to_email, subject, body)

                    if email_sent:
                        current_date_str = datetime.now().strftime('%Y-%m-%d')

                        # Ensure the column is of datetime type before updating it
                        df[f'Email 3 Sent'] = pd.to_datetime(df[f'Email 3 Sent'], errors='coerce')

                        # Now assign the current date as a datetime object
                        df.at[index, f'Email 3 Sent'] = pd.to_datetime(current_date_str)

                        excel_update_successful = update_excel(df, file_path, index, 3, current_date_str)
                        if not excel_update_successful:
                            logging.error(f"Error updating Excel after sending Email 3 to {to_email}.")
                            overall_success = False
                    else:
                        logging.error(f"Failed to send Email 3 to {to_email}.")
                        overall_success = False

        if overall_success:
            logging.info("All emails processed successfully.")
        else:
            logging.error("There were errors during email processing.")
        return overall_success
    else:
        logging.error("Failed to load data from Excel.")
        return False

# Entry point with flexible email number
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Schedule email sending.')
    parser.add_argument('--email_number', type=int, default=1, help='Email template number to use (1, 2, or 3)')
    args = parser.parse_args()
    schedule_emails()
