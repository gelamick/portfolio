from flask import Flask
import os

def create_app():
    """
    Factory function to create and configure the Flask application.

    This function initializes a Flask app, defines the path to the 'templates' directory,
    imports and registers blueprints, and returns the app instance.

    Key steps:
    1. Defines the absolute path of the 'templates' directory, ensuring Flask knows where to find the HTML templates.
    2. Initializes the Flask app with the 'template_folder' argument pointing to the 'templates' directory.
    3. Imports and registers the 'main' blueprint from the 'routes' module, allowing the app to use the defined routes.

    Returns:
        app (Flask): The Flask application instance with blueprints registered.
    """
    
    # Define the path to the 'templates' folder relative to this file
    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')

    # Initialize the Flask application
    app = Flask(__name__, template_folder=template_dir)

    # Import and register the blueprints
    from .routes import main as main_blueprint
    app.register_blueprint(main_blueprint)

    return app