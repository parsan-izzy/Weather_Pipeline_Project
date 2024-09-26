import requests
import logging
import sqlite3
import os
import random

# Set up logging
logging.basicConfig(filename='weather_pipeline.log', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Fetch data from the weather API
def fetch_data(city: str):
    api_key = "a177061a76d7bca369b0fd41f88046a3"
    api_url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Fetched data for {city}")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data for {city}: {e}")
        return None

# Convert Celsius to Fahrenheit
def convert_to_fahrenheit(celsius):
    return (celsius * 9/5) + 32

# Process the raw weather data
def process_data(data):
    try:
        if data:
            city = data['name']
            temp_celsius = round(data['main']['temp'])
            temp_fahrenheit = round(convert_to_fahrenheit(temp_celsius))
            weather_description = data['weather'][0]['description'].title()  # Capitalized for readability
            
            logging.info(f"Processed weather data for {city}")
            return city, temp_celsius, temp_fahrenheit, weather_description
        else:
            logging.error("No data to process.")
            return None
    except KeyError as e:
        logging.error(f"Error processing data: Missing field {e}")
        return None

# Save data to SQLite database
def save_to_db(city, temp_celsius, temp_fahrenheit, weather_description):
    try:
        db_path = r'C:\Users\Min Dator\Desktop\WEATHER 2\weather.db'
        logging.info(f"Saving data to: {db_path}")

        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create the table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS weather (
                city TEXT PRIMARY KEY,
                temperature_celsius INTEGER,
                temperature_fahrenheit INTEGER,
                description TEXT
            )
        ''')

        # Insert or replace the processed data into the table
        cursor.execute('''
            REPLACE INTO weather (city, temperature_celsius, temperature_fahrenheit, description)
            VALUES (?, ?, ?, ?)
        ''', (city, temp_celsius, temp_fahrenheit, weather_description))

        conn.commit()
        conn.close()
        logging.info(f"Weather data saved to database for {city}")
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")

# Main script logic
if __name__ == "__main__":
    # List of cities to randomly select from
    CITIES = ['Berlin', 'Arak', 'London', 'New York', 'Tokyo', 'Sydney']

    # Randomly select a city from the list
    city_name = random.choice(CITIES)
    logging.info(f"Randomly selected city: {city_name}")
    
    logging.info(f"Starting weather data pipeline for {city_name}")
    weather_data = fetch_data(city_name)
    
    if weather_data:
        # Process the raw data
        processed_data = process_data(weather_data)
        
        if processed_data:
            city, temp_celsius, temp_fahrenheit, weather_description = processed_data
            
            # Print processed data for debugging
            print(f"\nCity: {city}\nTemperature: {temp_celsius}°C ({temp_fahrenheit}°F)\nWeather: {weather_description}")
            
            # Save the processed data to the database
            save_to_db(city, temp_celsius, temp_fahrenheit, weather_description)
        else:
            print("No data to process.")
            logging.error(f"Processing failed for {city_name}")
    else:
        print("Failed to fetch weather data.")
        logging.error(f"Failed to fetch weather data for {city_name}")

logging.info("Pipeline finished")

