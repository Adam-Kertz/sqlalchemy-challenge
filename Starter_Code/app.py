# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc

# Import Flask
from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
base = automap_base()
# reflect the tables
base.prepare(autoload_with=engine)

# Save references to the tables
measurement = base.classes.measurement
station = base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# General Purpose Functions
#################################################

#Determine valid date ranges based on imported data
def first_last_dates(session):
    
    # Calculate first date in range
    first_date = session.query(func.min(measurement.date)).first()[0]
    
    # Calculate last date in range
    last_date = session.query(func.max(measurement.date)).first()[0]
    
    return first_date, last_date

# Calculate the date one year from the last date in data set
def last_year(session):

    # Find the most recent date in the data set.
    recent_date = session.query(measurement.date).order_by(desc(measurement.date)).first().date
    
    year = int(recent_date[0:4])              #Get year from first four digits of date and convert to integer
    last_year = str(year - 1)                 #Get the previous year and assign it to a variable as a string
    year_ago = last_year + recent_date[4:]    #Define the date one year ago
    
    return year_ago

# Define function to catch datetime exceptions
def is_valid_datetime(datetime_str):
    try:
        dt.datetime.strptime(datetime_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False
    
# Define function to correct URL date formatting (ensure all url date inputs are in YYYY-MM-DD format)
def datetime_format(datetime_str):
    
    # Convert string to datetime
    datetime_object = dt.datetime.strptime(datetime_str, "%Y-%m-%d")
    
    # Convert datetime back to string
    formatted_datetime = datetime_object.strftime("%Y-%m-%d")
    
    return formatted_datetime

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Get valid date range for user
    first_date, last_date = first_last_dates(session)
    
    session.close()
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/YYYY-MM-DD<br/>"
        f"/api/v1.0/YYYY-MM-DD/YYYY-MM-DD<br/>"
        f"<br/>Notes on last two routes:<br/>"
        f"Using one date in the URL defines a start date<br/>"
        f"Using two dates in the URL (separated by a '/') defines a start (first) and end date (second) <br/>"
        f"Dates must be between {first_date} and {last_date}"
    )

@app.route("/api/v1.0/precipitation")
def rain():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a year's worth of precipitation data as a list"""
    # Define the date one year ago from the most recent data point
    year_ago = last_year(session)

    # Perform a query to retrieve the date and precipitation scores
    rain_data = session.query(measurement.date,measurement.prcp).filter(measurement.date >= year_ago).all()

    session.close()

    # Convert list of tuples into a dictionary
    all_rain = dict(rain_data)

    return jsonify(all_rain)

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all station names"""
    # Query all stations
    station_list = session.query(station.station).all()

    session.close()

    # Extract station names from a list of tuples
    station_names = [row[0] for row in station_list]

    return jsonify(station_names)

@app.route("/api/v1.0/tobs")
def temperature():
    
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a year's worth of temperature data (from the most active station) as a list"""
    # Design a query to find the most active stations (i.e. which stations have the most rows?)
    # List the stations and their counts in descending order.
    station_query = session.query(measurement.station).\
    group_by(measurement.station).order_by(desc(func.count(measurement.station)))
    
    # Get the name of the station with most observations
    station_max = station_query.first()[0]
    
    # Query the last 12 months of temperature observation data for this station
    
    # Define the date one year ago from the most recent data point
    year_ago = last_year(session)
    
    # Get date and temperature date
    temp_data = session.query(measurement.date,measurement.tobs).\
    filter(measurement.date >= year_ago).filter(measurement.station == station_max).all()

    session.close()

    # Convert list of tuples into a dictionary
    temp_list = dict(temp_data)
    
    return jsonify(most_active_station = station_max, temperatures_by_date = temp_list)

@app.route("/api/v1.0/<start>")
def start(start):
    
    # Print error message for invalid dates
    if not is_valid_datetime(start):
        return "Invalid Date or API Route"
    # Ensure date is in correct format "YYYY-MM-DD"
    else:
        start = datetime_format(start)
    
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Get valid date range
    first_date, last_date = first_last_dates(session)
    
    # Catch instances where start date is outside of date range
    if start < first_date or start > last_date:
        return f"Date is outside of valid date range. Try a date between {first_date} and {last_date}"

    """Return the minimum, maximum, and average temperatures over a timeframe define by user-inputed start date"""
    # Query lowest, highest, and average temperature results since specified start date (and store as a tuple)
    temp_data = session.query(func.min(measurement.tobs),func.max(measurement.tobs),func.avg(measurement.tobs))\
    .filter(measurement.date >= start).all()[0]

    session.close()

    # Create key tuple for dictionary
    temp_keys = ("Minimum Temperature","Maximum Temperature","Average Temperature")

    # Create temperature dictionary
    temp_dict = {key: value for key, value in zip(temp_keys, temp_data)}

    return jsonify(start_date = start, temperature_data = temp_dict)

@app.route("/api/v1.0/<start>/<end>")
def start_end(start,end):
    
    # Print error message for invalid dates
    if not (is_valid_datetime(start) and is_valid_datetime(end)):
        return "Invalid Date Combination"
    # Ensure dates are in correct format "YYYY-MM-DD"
    else:
        start = datetime_format(start)
        end = datetime_format(end)
    
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Get valid date range
    first_date, last_date = first_last_dates(session)
    
    # Catch instances where start date is outside of date range
    if (start < first_date or end < first_date ) or (start > last_date or end > last_date) :
        return f"One or more dates outside of valid date range. Try start and end dates between {first_date} and {last_date}"
    elif start > end:
        return "The start date must come before (or be the same as) the end date"

    """Return the minimum, maximum, and average temperatures over a timeframe define by user-inputed start and end dates"""
    # Query lowest, highest, and average temperature results since specified start date
    temp_data = session.query(func.min(measurement.tobs),func.max(measurement.tobs),func.avg(measurement.tobs))\
    .filter(measurement.date >= start).filter(measurement.date <= end).all()[0]

    session.close()

    # Create key tuple for dictionary
    temp_keys = ("Minimum Temperature","Maximum Temperature","Average Temperature")

    # Create temperature dictionary
    temp_dict = {key: value for key, value in zip(temp_keys, temp_data)}

    # Create date range dictionary
    date_range = {"Start Date" : start, "End Date" : end}

    return jsonify(date_range = date_range, temperature_data = temp_dict)

if __name__ == '__main__':
    app.run(debug=True)