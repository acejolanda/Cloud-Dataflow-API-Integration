# Cloud Dataflow API Integration

In this data pipeline project, which is part of the "Gans" case study from WBS Coding School, I work as a Data Engineer at a company named "Gans" where I collect city data from the internet using web scraping and API integration. "Gans" is an e-scooter company operating in major cities worldwide where users can rent e-scooters through an app for a specific period of time. To optimize the use and availability of these e-scooters, the project involves gathering both static city information (such as their geographical location) and real-time weather data. Additionally, flight information, like arrival times at airports, is collected, as "Gans" plans to position scooters near airports for travelers (with light luggage), to easily access city centers by using an e-scooter instead of more expensive Taxis/Ubers.

The provided code enables data collection through APIs and stores the data in a remote SQL database. This data falls into two categories:

1. Static Data
  This data does not change and includes general information such as:
  - Cities: City name, country code, geographic coordinates (latitude and longitude)
  - Airports: Airport name, IATA code


2. Dynamic Data:
  Data that has to be updated or that is constantly changing. The dynamic data includes:
  - Weather: Temperature, wind speed, precipitation probability, precipitation amount, etc.
  - Incoming Flights: Flight number, arrival time, etc.

I created a local SQL database schema, containing tables for both static and dynamic data. The schema creation script is available in `project_schema.sql` and the schema itself is shown below:

![SQL database schema](schema.pdf)

The `set_up_database.py` script is designed to be executed once locally. It collects the static data and stores it in the corresponding tables in the SQL database.

For collecting dynamic data, such as weather and flight information, cloud functions I will use cloud run functions on Google Cloud Platform (GCP). These functions are defined in `cloud_functions.py` and can be deployed on GCP as cloud run functions. By deploying cloud functions, I can ensure that the corresponding tables always contain up-to-date data.
These functions can be scheduled using cron expressions to run at specific times/intervals. The functions for the data collection of the dynamic part of the project use API's like the functions of the static data category. However, the dynamic data functions must access the data of the static data category, which is why I use a data pipeline between the Google Cloud and the SQL database.
The newly collected dynamic data is appended to the respective tables via the data pipeline, and "Gans" can use this information to optimize the distribution and availability of e-scooters across cities (and their airports).
