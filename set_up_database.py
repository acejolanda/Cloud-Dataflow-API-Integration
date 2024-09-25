import pandas as pd
import requests
import mysql.connector
from keys_passwords import *
from datetime import datetime, timedelta

"""
This script is sets up the static tables within a pre-defined SQL database schema for the Gans study case project. The database must already be created in an SQL instance with tables structured according to specific cardinalities.
This script contains the "static" or "local" functions. By running this script once on a local machine, the following functions will be executed:
    - get_city_data(city_list)
    - city_info_to_sql(get_city_data_object)
    - airports()
    - cities_airports()
This will in turn populate the static tables (cities, airports, cities_airports) that will be used by dynamic cloud functions running on Google Cloud Platform (GCP) for scheduled data updates as a reference.
"""

city_list = ['Berlin', 'Hamburg', 'San Francisco'] # example city_list

def get_city_data(city_list):
    """
    Description:
        Collects information (city_name, country_code, latitude and longitude) from an external API and returns the information in a Pandas DataFrame.
    Parameters:
        city_list: list
            A list of city names in string format.
    Returns:
        pd.DataFrame
            cities DataFrame containing information about each city in a row.
    """
    cities_dictionary = {"city_name": [],
                         "country_code": [],
                         "latitude": [],
                         "longitude": []
    }

    for city in city_list:
        url = f"https://api.api-ninjas.com/v1/city?name={city}"

        city_response = requests.get(url, headers={'X-Api-Key': api_key_city})
        city_json = city_response.json()
        
        cities_dictionary["city_name"].append(city_json[0].get('name'))
        cities_dictionary["country_code"].append(city_json[0].get('country'))
        cities_dictionary["latitude"].append(city_json[0].get('latitude'))
        cities_dictionary["longitude"].append(city_json[0].get('longitude'))

    return pd.DataFrame(cities_dictionary)

def city_info_to_sql(city_df):
    """
    Inserts city data from a DataFrame into the 'cities' table of an SQL database. 

    Parameters:
    city_df : pd.DataFrame
        A DataFrame containing city information, with columns: 'city_name', 'country_code', 'latitude', 'longitude'.
    """
    cnx = mysql.connector.connect(
    user = 'root',
    password = sql_password,  
    host = sql_connection,  
    database = 'data_engineering'  
)

    cursor = cnx.cursor()

    for i in range(len(city_df)):
        city_name = city_df.loc[i, 'city_name']
        country_code = city_df.loc[i, 'country_code']
        latitude = float(city_df.loc[i, 'latitude'])
        longitude = float(city_df.loc[i, 'longitude'])
        
        query = ("""
                INSERT INTO cities (city_name, country_code, latitude, longitude)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    country_code = VALUES(country_code),
                    latitude = VALUES(latitude),
                    longitude = VALUES(longitude)
                """)
        
        cursor.execute(query, (city_name, country_code, latitude, longitude))

    cnx.commit()

    cursor.close()
    cnx.close()

city_df = get_city_data(city_list)
city_info_to_sql(city_df)

def airports():
    """
    Inserts airports data from a DataFrame into the 'airports' table of an SQL database. 
    """
    cnx = mysql.connector.connect(
        user = 'root',
        password = sql_password,  
        host = sql_connection,  
        database = 'data_engineering'  
    )

    airports_dataframes = []

    cursor = cnx.cursor()
    query = ("SELECT latitude, longitude  FROM cities")
    cursor.execute(query)
    cities = cursor.fetchall()

    for latitude, longitude in cities:
        url = "https://aerodatabox.p.rapidapi.com/airports/search/location"

        querystring = {"lat":{latitude},"lon":{longitude},"radiusKm":"50","limit":"10","withFlightInfoOnly":"true"}

        headers = {
	        "x-rapidapi-key": flights_api_key,
	        "x-rapidapi-host": "aerodatabox.p.rapidapi.com"
        }

        airports_response = requests.get(url, headers=headers, params=querystring)
        airports_json = airports_response.json()

        airports_dict = {
            "airport_iata": [],
            "airport_name": []
        }

        for i in range(0, len(airports_json['items'])):
            airports_dict["airport_iata"].append(airports_json['items'][i].get('iata'))
            airports_dict["airport_name"].append(airports_json['items'][i].get('name'))

        airports_dataframes.append(pd.DataFrame(airports_dict))
    
    combined_dataframe = pd.concat(airports_dataframes, ignore_index=True)

    insert_query = ("""
                    INSERT INTO airports (airport_iata, airport_name)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        airport_iata = VALUES(airport_iata),
                        airport_name = VALUES(airport_name)
                    """)

    airports_list = []
    for i, row in combined_dataframe.iterrows():
        airports_list.append((
            row['airport_iata'],
            row['airport_name']
        ))
    
    cursor.executemany(insert_query, airports_list)
    cnx.commit()
    cursor.close()
    cnx.close()

airports()

def cities_airports():
    """
    Inserts several airport data for each city from a DataFrame into the 'cities_airports' table of an SQL database. 
    Serves as a bridge table between the cities table and the airports table (one city can have many airports, leading to a many-to-many relationship).
    """

    cnx = mysql.connector.connect(
        user='root',
        password=sql_password,  
        host = sql_connection,  
        database='data_engineering'  
    )

    cities_airports_dataframes = []

    cursor = cnx.cursor()
    query = ("SELECT city_id, latitude, longitude  FROM cities")
    cursor.execute(query)
    cities = cursor.fetchall()

    for city_id, latitude, longitude in cities:
        url = "https://aerodatabox.p.rapidapi.com/airports/search/location"

        querystring = {"lat":{latitude},"lon":{longitude},"radiusKm":"50","limit":"10","withFlightInfoOnly":"true"}

        headers = {
	        "x-rapidapi-key": flights_api_key,
	        "x-rapidapi-host": "aerodatabox.p.rapidapi.com"
        }

        cities_airports_response = requests.get(url, headers=headers, params=querystring)
        cities_airports_json = cities_airports_response.json()

        cities_airports_dict = {
            "city_id": [],
            "airport_iata": [],
        }

        for i in range(0, len(cities_airports_json['items'])):
            cities_airports_dict["city_id"].append(city_id)
            cities_airports_dict["airport_iata"].append(cities_airports_json['items'][i].get('iata'))


        cities_airports_dataframes.append(pd.DataFrame(cities_airports_dict))
    
    combined_dataframe = pd.concat(cities_airports_dataframes, ignore_index=True)

    insert_query = ("""
                    INSERT INTO cities_airports (city_id, airport_iata)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        city_id = VALUES(city_id),
                        airport_iata = VALUES(airport_iata)
                    """)

    cities_airports_list = []
    for i, row in combined_dataframe.iterrows():
        cities_airports_list.append((
            row['city_id'],
            row['airport_iata']
        ))
    
    cursor.executemany(insert_query, cities_airports_list)
    cnx.commit()
    cursor.close()
    cnx.close()

cities_airports()

