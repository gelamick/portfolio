from flask import Blueprint, render_template, redirect, url_for, flash, get_flashed_messages
from app.scheduler import schedule_emails
import logging

# Define a Blueprint for the main application
main = Blueprint('main', __name__)

@main.route('/')
def index():
    """
    Render the index page and display any flash messages.
    
    This route handles rendering the home page (`index.html`). It checks if any 
    flash messages were set during the previous requests (such as success or 
    error notifications) and passes them to the template to be displayed.
    
    Returns:
        A rendered template of the index page, including any flash messages.
    """
    messages = get_flashed_messages(with_categories=True)  # Retrieve flash messages with their categories (e.g., success, error)
    return render_template('index.html', messages=messages)

@main.route('/start_campaign', methods=['POST'])
def start_campaign():
    """
    Start the email campaign by scheduling emails and handle success or error notifications.
    
    This route is triggered by a POST request to initiate the email campaign. It calls 
    the `schedule_emails()` function to attempt sending emails to the prospects.
    
    - If the campaign is successful, a success flash message is displayed.
    - If some emails fail to send, a warning flash message is shown.
    - If there is an error during the process, an error flash message is displayed.
    
    In all cases, the user is redirected back to the index page after the process.
    
    Returns:
        A redirect to the index route, showing the status of the campaign (success or failure).
    """
    try:
        # Attempt to schedule emails and check if the operation was successful
        campaign_success = schedule_emails()  
        
        if campaign_success:
            flash('Campaign successfully done!', 'success')  # Show success message if all emails were sent
            logging.info("Email campaign started successfully.")
        else:
            flash('Some emails were not sent successfully.', 'warning')  # Show warning if some emails failed
            logging.warning("Email campaign completed with partial errors.")
    except Exception as e:
        # Handle any exceptions that occur and log the error
        flash(f'Error starting campaign: {str(e)}', 'error')  # Show error message if campaign failed completely
        logging.error(f"Error starting email campaign: {e}")
    
    # Redirect the user back to the index page
    return redirect(url_for('main.index'))