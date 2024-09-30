import pandas as pd
import requests
import mysql.connector
from keys_passwords import *
from datetime import datetime, timedelta



"""
This script contains the following cloud functions:
    - population_data_to_sql(request)
    - get_weather(request)
    - flights(request)
These functions manage automated data pipelines for population, weather, and flight information.
They are intended to run on Google Cloud Platform (GCP),
allowing for seamless execution without the need to manage servers.
The functions are triggered through GCP's scheduler framework:
The population_data_to_sql function runs automatically on January 1st each year to update population data. The get_weather and flights function run daily at 6 AM to gather current weather and flight data, respectively.
Each function connects to a local MySQL database to collect, store, and update the relevant data.
"""

def population_data_to_sql(request):
    """
    Description:
        Retrieves city data from existing "cities" table in the SQL database and fetches for each city the population data from an external API.
        The collected population data is then inserted into the "populations" table of the SQL database, updating any existing entries if necessary.
    Parameters:
        request: The request object for triggering the function on GCP (not used within the function body).
    Returns:
        str: Confirmation message when data is successfully added to the database.
    """
    cnx = mysql.connector.connect(
        user = 'root',
        password = sql_password,  
        host = sql_connection,  
        database = 'data_engineering'  
    )
    
    cursor = cnx.cursor()

    # we get the city_id as foreign key from already existing cities table in SQL
    query = ("SELECT city_id, city_name FROM cities")
    cursor.execute(query)
    cities = cursor.fetchall()

    for city_id, city_name in cities:
        url = f"https://api.api-ninjas.com/v1/city?name={city_name}"
        city_response = requests.get(url, headers={'X-Api-Key': api_key_city})
        city_json = city_response.json()

        if city_json:
            population = city_json[0].get('population')
            timestamp_population = datetime.today().strftime('%Y-%m-%d')

            insert_query = ("""
                            INSERT INTO populations (city_id, population, timestamp_population) 
                            VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                population = VALUES(population), timestamp_population = VALUES(timestamp_population)
                        """) # with ON DUPLICATE we update the data if it already exists, otherwise it will be created
            cursor.execute(insert_query, (city_id, population, timestamp_population))

    cnx.commit()
    cursor.close()
    cnx.close()
    return "Data successfully added"

def get_weather(request):
    """
    Description:
        Collects weather forecast data for cities stored in the SQL database by using their latitude and longitude coordinates. 
        For each city, it collects weather information every 6 hours via the OpenWeather API, including temperature, main forecast, description, wind speed, precipitation probability, and rain amount. 
        The retrieved data is then inserted into the "city_weather" table in the SQL database.
    Parameters:
        request: The request object for triggering the function on GCP (not used within the function body).
    Returns:
        str: Confirmation message when data is successfully added to the database.
    """
    cnx = mysql.connector.connect(
        user = 'root',
        password = sql_password,  
        host = sql_connection,  
        database = 'data_engineering'  
    )

    city_weather_dataframes = []

    cursor = cnx.cursor()
    query = ("SELECT city_id, latitude, longitude  FROM cities")
    cursor.execute(query)
    cities = cursor.fetchall()


    for city_id, latitude, longitude in cities:
        url = f"http://api.openweathermap.org/data/2.5/forecast?lat={latitude}&lon={longitude}&appid={api_key_weather}&units=metric"
        forecast_response = requests.get(url)
        weather_json = forecast_response.json()

        weather_dict = {
                    "city_id": [],
                    "temperatures": [],
                    "main_forecasts" : [],
                    "descriptions" : [],
                    "wind_speeds" : [],
                    "date_times" : [],
                    "chance of precipitation" : [],
                    "rain_amount" : []
    }
        for i in range(0, len(weather_json['list']), 2): # collect information every 6 hours
            weather_dict["city_id"].append(city_id)
            weather_dict["temperatures"].append(weather_json['list'][i]['main'].get('temp'))
            weather_dict["main_forecasts"].append(weather_json['list'][i]['weather'][0].get('main'))
            weather_dict["descriptions"].append(weather_json['list'][i]['weather'][0].get('description'))
            weather_dict["wind_speeds"].append(weather_json['list'][i]['wind'].get('speed'))
            weather_dict["date_times"].append(weather_json['list'][i].get('dt_txt'))
            weather_dict["chance of precipitation"].append(weather_json['list'][i].get('pop'))
            weather_dict["rain_amount"].append(weather_json['list'][i].get('rain', {}).get('3h', 0))

        city_weather_dataframes.append(pd.DataFrame(weather_dict))
        
    combined_dataframe = pd.concat(city_weather_dataframes, ignore_index=True)
    
    # insert weather dataframe to SQL table 
    insert_query = f"""
        INSERT INTO city_weather
        (city_id, temperature, main_forecast, description, wind_speed, date_time, chance_of_precipitation, rain_amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    weather_list = []
    
    for i, row in combined_dataframe.iterrows():
        weather_list.append((
            row['city_id'],
            row['temperatures'],
            row['main_forecasts'],
            row['descriptions'],
            row['wind_speeds'],
            row['date_times'],
            row['chance of precipitation'],
            row['rain_amount']
        ))

    cursor.executemany(insert_query, weather_list)
    cnx.commit()
    cursor.close()
    cnx.close()
    return "Data successfully added"

def flights(request):
    """
    Description:
        Collects arrival flight data for airports stored in the SQL database ("airports" table) using their IATA codes.
        For each airport, it collects flight information in two time windows (00:00-11:59 and 12:00-23:59) for the following day via the Aerodatabox API.
        The collected data, including the airport IATA code, flight number, and scheduled arrival time, is inserted into the "flights" table in the SQL database.
    Parameters:
        request: The request object for triggering the function on GCP (not used within the function body).
    Returns:
        str: Confirmation message when data is successfully added to the database.
    """
    cnx = mysql.connector.connect(
        user = 'root',
        password = sql_password,  
        host = sql_connection,  
        database = 'data_engineering'  
    )

    flights_dataframes = []

    cursor = cnx.cursor()
    query = ("SELECT airport_iata FROM airports")
    cursor.execute(query)
    airports = cursor.fetchall()

    time_windows = [("00:00", "11:59"), ("12:00", "23:59")]
    next_day = (datetime.now() + timedelta(days=1)).date()

    for airport_iata_tuple in airports:
        airport_iata = airport_iata_tuple[0]
        #print(airport_iata)

        for time in time_windows:
            time_1, time_2 = time

            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/iata/{airport_iata}/{next_day}T{time_1}/{next_day}T{time_2}"

            querystring = {
                "withLeg": "false",
                "direction": "Arrival",  
                "withCancelled": "false",
                "withCodeshared": "true",
                "withCargo": "false",
                "withPrivate": "false",
                "withLocation": "false"
            }

            headers = {
                "x-rapidapi-key": flights_api_key,
                "x-rapidapi-host": "aerodatabox.p.rapidapi.com"
            }

            try:
                arrival_flights_response = requests.get(url, headers=headers, params=querystring)
                arrival_flights_json = arrival_flights_response.json()

                flights_dict = {
                                "arrival_iata": [],
                                "flight_num": [],
                                "arrival_time_scheduled": []
                }

                for flight in arrival_flights_json.get('arrivals', []):
                    flights_dict["arrival_iata"].append(airport_iata)
                    flights_dict["flight_num"].append(flight.get('number'))

                    scheduled_time = flight['movement']['scheduledTime'].get('local')

                    flights_dict["arrival_time_scheduled"].append(scheduled_time[:19])

                flights_dataframes.append(pd.DataFrame(flights_dict))

            except requests.exceptions.RequestException as error:
                print(f"API request error for airport with iata: {airport_iata}: {error}")

    combined_dataframe = pd.concat(flights_dataframes, ignore_index=True)

    insert_query = f"""
        INSERT INTO flights
        (arrival_iata, flight_num, arrival_time_scheduled)
        VALUES (%s, %s, %s);
    """

    flights_list = []
    for i, row in combined_dataframe.iterrows():
        flights_list.append((
            row['arrival_iata'],
            row['flight_num'],
            row['arrival_time_scheduled']
        ))

    cursor.executemany(insert_query, flights_list)
    cnx.commit()
    cursor.close()
    cnx.close()
    return "Data successfully added"

