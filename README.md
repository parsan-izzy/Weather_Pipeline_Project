# Weather Data Pipeline

## Overview
This project is a Python-based weather data pipeline that fetches real-time weather information from an API, processes it, and stores the data in a SQLite database. The data is retrieved for a random city from a pre-defined list of cities. The program logs each step and can be scheduled to run at regular intervals using a scheduler.

## Features
- Fetches weather data for random cities using OpenWeatherMap API.
- Processes the data, including converting temperature from Celsius to Fahrenheit.
- Stores the processed weather data in a local SQLite database.
- Logs all events and errors to a log file (`weather_pipeline.log`).
- Can be scheduled to run automatically.

## Requirements
- Python 3.x
- Required Python libraries:
  - `requests`
  - `sqlite3`
  - `logging`

