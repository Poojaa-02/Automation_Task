import os
import sys
from datetime import datetime, timedelta, timezone
import pymysql.cursors
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Creation of Absolute path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

# Import configuration and constants
from ConfigFiles.config import API_KEY, cities, db_config

# Established connection and Table creation if not exists and update if exists 
def create_forecast_table(connection):
    try:
        with connection.cursor() as cursor:
            create_table_query = """
                CREATE TABLE IF NOT EXISTS Forecasted_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    city VARCHAR(255),
                    date DATE,
                    temperature FLOAT,
                    humidity INT,
                    weather_condition VARCHAR(255),
                    UNIQUE KEY unique_entry (city, date)
                )
            """
            cursor.execute(create_table_query)

        connection.commit()
        print("Forecasted_data table created successfully.")

    except pymysql.MySQLError as e:
        print(f"Error creating table: {e}")

def create_actual_table(connection):
    try:
        with connection.cursor() as cursor:
            create_table_query = """
                CREATE TABLE IF NOT EXISTS Actual_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    city VARCHAR(255),
                    date DATE,
                    temperature FLOAT,
                    humidity INT,
                    weather_condition VARCHAR(255),
                    UNIQUE KEY unique_entry (city, date)
                )
            """
            cursor.execute(create_table_query)

        connection.commit()
        print("Actual_data table created successfully.")

    except pymysql.MySQLError as e:
        print(f"Error creating table: {e}")

# Fetching data from the web browser interaction with cromedriver
def fetch_forecast(driver, city):
    driver.get(f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={API_KEY}")
    data = driver.execute_script("return JSON.parse(document.body.innerText)")
    print(f"Forecast data fetched successfully for {city}.")
    return data

def fetch_actual_data(driver, city):
    driver.get(f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}")
    data = driver.execute_script("return JSON.parse(document.body.innerText)")
    print(f"Actual data fetched successfully for {city}.")
    return data

# Saving all the data into the python readable form in list temp
def save_forecast_to_database(city, forecast_data, cursor, connection):
    for entry in forecast_data["list"]:
        date_str = entry["dt_txt"]
        date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").date()

        tomorrow = datetime.now(timezone.utc).date() + timedelta(days=1)
        five_days_later = tomorrow + timedelta(days=5)

        if tomorrow <= date < five_days_later:
            temperature = entry["main"]["temp"] - 273.15
            humidity = entry["main"]["humidity"]
            condition = entry["weather"][0]["description"]

            query_check = "SELECT id FROM Forecasted_data WHERE city = %s AND date = %s"
            values_check = (city, date)
            cursor.execute(query_check, values_check)
            existing_data = cursor.fetchone()

            if existing_data:
                existing_data_id = existing_data[0]
                query_update = "UPDATE Forecasted_data SET temperature = %s, humidity = %s, weather_condition = %s WHERE id = %s"
                values_update = (temperature, humidity, condition, existing_data_id)
                cursor.execute(query_update, values_update)
                print(f"Forecast data updated successfully for {city} on {date}.")
            else:
                query_insert = "INSERT INTO Forecasted_data (city, date, temperature, humidity, weather_condition) VALUES (%s, %s, %s, %s, %s)"
                values_insert = (city, date, temperature, humidity, condition)
                cursor.execute(query_insert, values_insert)
                print(f"Forecast data inserted successfully for {city} on {date}.")

            connection.commit()

def save_actual_data_to_database(city, actual_data, cursor, connection):
    try:
        date = datetime.utcnow().date()
        temperature = actual_data['main']['temp'] - 273.15
        humidity = actual_data['main']['humidity']
        condition = actual_data['weather'][0]['description']

        query = "INSERT IGNORE INTO Actual_data (city, date, temperature, humidity, weather_condition) VALUES (%s, %s, %s, %s, %s)"
        values = (city, date, temperature, humidity, condition)

        cursor.execute(query, values)
        connection.commit()
        print(f"Actual data saved successfully for {city}.")

    except KeyError:
        print(f"Error: Missing data for {city}. Please check with the API provider.")

def main():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(options=chrome_options)

    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    create_forecast_table(connection)
    create_actual_table(connection)

    for city in cities:
        forecast_data = fetch_forecast(driver, city)
        actual_data = fetch_actual_data(driver, city)

        if forecast_data["cod"] == "200":
            save_forecast_to_database(city, forecast_data, cursor, connection)
        else:
            print(f"Failed to fetch forecast data for {city}. Error code: {forecast_data['cod']}")

        if 'cod' in actual_data and actual_data['cod'] == 200:
            save_actual_data_to_database(city, actual_data, cursor, connection)
        else:
            print(f"Failed to fetch actual data for {city}. Error code: {actual_data.get('cod')}")

    cursor.close()
    connection.close()
    driver.quit()

if __name__ == "__main__":
    main()
print("Sucessfully Added all the Data Into the Tables")