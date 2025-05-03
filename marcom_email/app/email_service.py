import os
import pandas as pd
import smtplib
import base64
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formataddr
import logging
from datetime import datetime
from dotenv import load_dotenv
from mailjet_rest import Client


load_dotenv()  # Load environment variables from .env file

# Mailjet API credentials from environment variables
MAILJET_API_KEY = os.getenv('MAILJET_API_KEY')
MAILJET_SECRET_KEY = os.getenv('MAILJET_SECRET_KEY')
EMAIL_USER = os.getenv("EMAIL_USER")

"""
This application automates the process of sending marketing emails to prospects listed in an Excel file.
It checks if enough time has passed since the last email sent and generates personalized email content
based on templates. The emails can include an optional PDF attachment and are tracked within the Excel file.
It logs any errors or successes and updates the Excel file with the email sending status.
"""

def is_valid_email(email):
    """Check the validity of email addresses using a regular expression pattern."""
    email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(email_regex, email) is not None

def load_excel_data(file_path):
    """Load data from an Excel file and handle potential errors."""
    try:
        df = pd.read_excel(file_path)
        if df.empty:
            logging.error(f"Excel file is empty: {file_path}")
            return None
        logging.info(f"Loaded DataFrame from {file_path}.")
        logging.info(f"DataFrame preview:\n{df.head()}")  # Print first 5 rows to avoid large output
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return None
    except Exception as e:
        logging.error(f"Error loading Excel file: {e}")
        return None

def normalize_date(date_str):
    """Convert string to datetime if possible, handling multiple date formats."""
    try:
        return pd.to_datetime(date_str, errors='coerce')
    except Exception as e:
        logging.error(f"Error converting date: {e}")
        return None

def is_ready_for_next_email(df, index, email_number):
    """Check if an email can be sent based on the status of previous emails."""
    try:
        # Check if the previous email was sent
        previous_email_sent_column = {2: 'Email 1 Sent', 3: 'Email 2 Sent'}.get(email_number)
        if not previous_email_sent_column:
            logging.error(f"Invalid email number: {email_number}.")
            return False
        
        previous_email_sent = df.at[index, previous_email_sent_column]
        if previous_email_sent != 'Sent':
            logging.info(f"Previous email {email_number - 1} not sent for index {index}.")
            return False

        # Check if enough time has passed since the previous email
        previous_email_sent_date = normalize_date(df.at[index, f'Email {email_number - 1} Date'])
        if not previous_email_sent_date:
            logging.error(f"Invalid date format for index {index}, email {email_number - 1}")
            return False

        days_since_last_sent = (datetime.now() - previous_email_sent_date).days
        return days_since_last_sent >= 1
    except Exception as e:
        logging.error(f"Error checking readiness for email {email_number} at index {index}: {e}")
        return False

def update_excel(df, file_path, index, email_number, sent_datetime):
    """
    Update the Excel file with the current datetime for the specific email number.
    
    Args:
        df (DataFrame): The DataFrame containing the prospect data.
        file_path (str): Path to the Excel file.
        index (int): Index of the row to update.
        email_number (int): The email number to update (1, 2, or 3).
        sent_datetime (datetime): The datetime to set for the email.
        
    Returns:
        bool: True if update is successful, False otherwise.
    """
    try:
        # Update the 'Email X Sent' column with the current datetime
        df.at[index, f'Email {email_number} Sent'] = sent_datetime

        # Ensure all 'Email X Sent' columns are datetime type
        for col in df.columns:
            if col.startswith('Email') and 'Sent' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Use ExcelWriter to handle datetime formatting
        with pd.ExcelWriter(file_path, engine='openpyxl', datetime_format='yyyy-mm-dd HH:MM:SS') as writer:
            df.to_excel(writer, index=False)

        logging.info(f"Excel file updated successfully at {file_path}.")
        return True
    except Exception as e:
        logging.error(f"Error updating Excel file: {e}")
        return False

def get_template_path(template_name):
    """Construct the path to an email template."""
    return os.path.join(os.path.dirname(__file__), 'templates', template_name)

def read_email_template(file_path, prospect):
    """Read and format the email template for a given prospect."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            template = file.read()
        return template.format(
            title=prospect.get('Title', ''),
            first_name=prospect.get('First Name', ''),
            last_name=prospect.get('Last Name', ''),
            company_name=prospect.get('Company Name', '')
        )
    except Exception as e:
        logging.error(f"Error reading email template: {e}")
        return ""

def read_signature(file_path):
    """Read and return the email signature, replacing logo placeholder with a direct URL to the logo, and including alt text."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            signature = file.read()
        # Remplacer {{logo_url}} par l'URL publique de l'image hébergée avec alt text pour le logo
        logo_html = '<img src="specify logo image link" alt="write name" style="width: 300px; height: auto; max-width: 100%;">'
        return signature.replace('{{logo_url}}', logo_html)
    except Exception as e:
        logging.error(f"Error reading signature file: {e}")
        return ""

def send_email(to_email, subject, body, pdf_attachment=None):
    """Send an email using the Mailjet API, with an optional PDF attachment."""
    if not is_valid_email(to_email):
        logging.error(f"Invalid email address: {to_email}")
        return False

    try:
        # Initialize the Mailjet client
        mailjet = Client(auth=(MAILJET_API_KEY, MAILJET_SECRET_KEY), version='v3.1')

        # Create the base email data
        email_data = {
            'Messages': [
                {
                    'From': {
                        'Email': EMAIL_USER,  # Sender's email
                        'Name': 'write sender name'  # Sender's name
                    },
                    'To': [
                        {
                            'Email': to_email,  # Recipient's email
                        }
                    ],
                    'Subject': subject,  # Email subject
                    'HTMLPart': body  # HTML body content
                }
            ]
        }

        # If there's a PDF attachment, add it
        if pdf_attachment and os.path.isfile(pdf_attachment):
            with open(pdf_attachment, 'rb') as pdf_file:
                pdf_content = pdf_file.read()
            attachment = {
                'ContentType': 'application/pdf',
                'Filename': os.path.basename(pdf_attachment),
                'Base64Content': base64.b64encode(pdf_content).decode('utf-8')  # Convert to base64 for Mailjet
            }
            email_data['Messages'][0]['Attachments'] = [attachment]

        # Send the email via Mailjet
        result = mailjet.send.create(data=email_data)

        # Check the response status
        if result.status_code == 200:
            logging.info(f"Email successfully sent to: {to_email}")
            return True
        else:
            logging.error(f"Failed to send email to {to_email}. Status code: {result.status_code}, Error: {result.json()}")
            return False
    except Exception as e:
        logging.error(f"Error while sending email to {to_email}: {e}")
        return False
    
def generate_email_content(prospect, email_number):
    """Generate email content based on the prospect and email number."""
    # Get the email template path dynamically based on email_number
    template_path = get_template_path(f"email_template_{email_number}.html")
    if not os.path.isfile(template_path):
        logging.error(f"Template file not found: {template_path}")
        return "Error", "Could not find the email template."

    # Read the email template and inject the prospect data
    body = read_email_template(template_path, prospect)

    # Safely extract the company name (default to 'Your Company' if missing)
    company_name = prospect.get('Company Name', 'Your Company')

    # Define subject based on email number
    subject = {
        1: f"{company_name} email title", 
        2: f"RE: {company_name} email title (keep same title)", 
        3: f"RE: RE: {company_name} email title (keep same title)"
    }.get(email_number, "No Subject")

    logging.info(f"Generated subject for Email {email_number}: {subject}")
    
    # Read the email signature
    signature_path = get_template_path('signature.html')
    if not os.path.isfile(signature_path):
        logging.error(f"Signature file not found: {signature_path}")
        return "Error", "Could not find the email signature."

    signature = read_signature(signature_path)
    if signature:
        logging.info(f"Signature added for email {email_number}")
    
    # Return the subject and body with the signature appended
    return subject, body + signature

def send_emails_from_excel(file_path, email_number, pdf_attachment=None):
    """Send emails based on the data from the Excel file."""
    df = load_excel_data(file_path)
    if df is None:
        logging.error("No data found, aborting email send process.")
        return
    
    for index, row in df.iterrows():
        prospect_email = row.get('Email')
        if not is_valid_email(prospect_email):
            logging.error(f"Invalid email address: {prospect_email}")
            continue

        # Check if this email is ready to be sent
        if not is_ready_for_next_email(df, index, email_number):
            logging.info(f"Skipping email {email_number} for {prospect_email}.")
            continue

        subject, body = generate_email_content(row, email_number)

        # Send the email
        success = send_email(prospect_email, subject, body, pdf_attachment)
        if success:
            current_datetime = datetime.now()
            update_excel(df, file_path, index, email_number, current_datetime)
        else:
            write_error_to_excel(file_path, index, email_number)

def write_error_to_excel(file_path, index, email_number):
    """Log the error in the Excel file for the specific prospect."""
    try:
        df = pd.read_excel(file_path)
        df.at[index, f'Email {email_number} Sent'] = 'Error'
        df.to_excel(file_path, index=False)
        logging.info(f"Logged error for {email_number} at index {index}.")
    except Exception as e:
        logging.error(f"Error logging error to Excel: {e}")

