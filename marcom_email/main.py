"""
This script serves as the entry point to run the Flask application.

Steps:
1. Imports the `create_app` function from the `app` module to initialize the Flask application instance.
2. Sets a secret key used by Flask for securely signing the session cookie. (In production, this should be replaced by a strong, unique secret).
3. If the script is executed directly (rather than imported), it runs the Flask development server with `debug` mode enabled, providing real-time error reporting and code reloading during development.

Attributes:
    app (Flask): The Flask application instance created by `create_app()`.
    secret_key (str): A hardcoded secret key for session management (should be securely generated in production).
"""
from app import create_app

# Initialize the Flask application
app = create_app()

# Set the secret key for session management
app.secret_key = '1234567'  # Replace this with a unique and secret key

if __name__ == "__main__":
    app.run(debug=True)
