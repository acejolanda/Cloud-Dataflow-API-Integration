/***************************
Setting up the environment
***************************/

-- Drop the database/schema if it already exists
DROP DATABASE IF EXISTS data_engineering ;

-- Create the database/schema
CREATE DATABASE data_engineering;

-- Use the database/schema
USE data_engineering;



/***************************
Creating the first table
***************************/

-- Create the 'cities' table
CREATE TABLE cities (
    city_id INT AUTO_INCREMENT, # Automatically generated ID for each city
    city_name VARCHAR(255) NOT NULL, # Name of the city
    country_code VARCHAR(255),
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    PRIMARY KEY (city_id), # Primary key to uniquely identify each city
    UNIQUE (city_name, country_code) # necessary to avoid duplicates
);

SELECT * FROM cities;
-- it's empty at the moment

CREATE TABLE populations (
    population_id INT AUTO_INCREMENT, -- Automatically generated ID for each population
    city_id INT,
    population INT NOT NULL,
    timestamp_population DATETIME,
    PRIMARY KEY (population_id), -- Primary key to uniquely identify each population
    FOREIGN KEY (city_id) REFERENCES cities(city_id),
    UNIQUE (city_id, timestamp_population)
);

SELECT * FROM populations;

CREATE TABLE IF NOT EXISTS city_weather (
	weather_id INT AUTO_INCREMENT,
	city_id INT,
	temperature FLOAT,
	main_forecast VARCHAR(255),
	description VARCHAR(255),
	wind_speed FLOAT,
	date_time DATETIME,
	chance_of_precipitation FLOAT,
	rain_amount FLOAT,
	PRIMARY KEY (weather_id),
	FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

SELECT * FROM city_weather;


CREATE TABLE airports (
    airport_iata VARCHAR(5),
    airport_name VARCHAR(255),
    PRIMARY KEY (airport_iata)
);

SELECT * FROM airports;

CREATE TABLE cities_airports (
    city_airport_id INT AUTO_INCREMENT, 
    city_id INT,
    airport_iata VARCHAR(5),
    PRIMARY KEY (city_airport_id),
    FOREIGN KEY (city_id) REFERENCES cities(city_id),
    FOREIGN KEY (airport_iata) REFERENCES airports(airport_iata),
    UNIQUE (city_id, airport_iata)
);

SELECT * FROM cities_airports;

CREATE TABLE flights (
    flight_id INT AUTO_INCREMENT,
	arrival_iata VARCHAR(5),
    flight_num VARCHAR(25),
	arrival_time_scheduled DATETIME,
    PRIMARY KEY (flight_id),
    FOREIGN KEY (arrival_iata) REFERENCES airports(airport_iata)
);

SELECT * FROM flights;








