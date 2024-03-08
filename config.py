import os
from dotenv import load_dotenv
 
# Load environment variables from .env
load_dotenv()
 
# API Key
API_KEY = os.environ.get('API_KEY')
 
# Cities
cities_str = os.environ.get("Cities")
if cities_str:
    cities = cities_str.split(",")
else:
    print("Error: 'CITIES' environment variable not set.")
    
# Database Configuration
db_config = {
    'user': os.environ.get('DataBase_User'),
    'password': os.environ.get('DataBase_Password'),
    'host': os.environ.get('localhost'),
    'database': os.environ.get('Your_DataBase'),
}