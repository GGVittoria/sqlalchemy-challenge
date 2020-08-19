import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    return (
        f"Welcome to Surfs Up in Hawaii - Climate Analysis and Exploration!<br/>"
        f"Available Routes:<br/>"
        f"<br/>"
        f"The precipitation data for the last 12 months of data:<br/>"        
        f"/api/v1.0/precipitation<br/>"
        f"<br/>"
        f"List of stations:<br/>"
        f"/api/v1.0/stations<br/>"
        f"<br/>"
        f"List of temperature observations of the most active station for the last year of data:<br/>"        
        f"/api/v1.0/tobs<br/>"
        f"<br/>"            
        f"Min, Max. and Avg. temperatures for given start date:<br/>"
        f"/api/v1.0/[start_date format:yyyy-mm-dd]<br/>"
        f"<br/>"
        f"Min. Max. and Avg. temperatures for given start and end date:<br/>"        
        f"/api/v1.0/[start_date format:yyyy-mm-dd]/[end_date format:yyyy-mm-dd]<br/>"
        f"<br/>"
        f"i.e. <a href='/api/v1.0/2017-08-01/2017-08-16' target='_blank'>/api/v1.0/2017-08-01/2017-08-16</a>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    """Last Year of Percipitation Data"""
    session = Session(engine)
    
    # Find last date in database from Measurements 
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first().date

    # Convert last date string to date
    last_date = dt.datetime.strptime(last_date, "%Y-%m-%d")

    # Calculate date one year after last date using timedelta datetime function
    first_date = last_date - timedelta(days=365)

    # Perform a query to retrieve the precipitation data
    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= first_date).order_by(Measurement.date).all()

    session.close()

    # Convert list of tuples into normal list
    all_precipitation = list(np.ravel(results))
    
    # Convert the list to Dictionary
    all_precipitation = {all_precipitation[i]: all_precipitation[i + 1] for i in range(0, len(all_precipitation), 2)} 

    return jsonify(all_precipitation)

@app.route("/api/v1.0/stations")
def stations():
    """List of Weather Stations"""
    session = Session(engine)

    # select station names from stations table
    results = session.query(Station.station).\
                 order_by(Station.station).all()

    session.close()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    """Temperature Observations for Top Station for Last Year"""
    session = Session(engine)
    
    #find last date in database from Measurements 
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first().date

    #convert last date string to date
    last_date = dt.datetime.strptime(last_date, "%Y-%m-%d")

    #calculate date one year after last date using timedelta datetime function
    first_date = last_date - timedelta(days=365)
    
    # List the stations and the counts in descending order.
    station_counts = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()   
        
    # Create top station variable from tuple
    top_station = (station_counts[0])
    top_station = (top_station[0])
    
    # Using the station id from the previous query, calculate the lowest temperature recorded, 
    # highest temperature recorded, and average temperature of the most active station?
    session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
    filter(Measurement.station == top_station).all()
    
    # Choose the station with the highest number of temperature observations.
    # Query the last 12 months of temperature observation data for this station and plot the results as a histogram
    results = session.query(Measurement.date,  Measurement.tobs).\
    filter(Measurement.station == top_station).filter(Measurement.date >= first_date).all()
    
    session.close()

    # Convert list of tuples into normal list
    tobs = list(np.ravel(results))

    # Convert the list to Dictionary
    tobs = {tobs[i]: tobs[i + 1] for i in range(0, len(tobs), 2)} 
   
    return jsonify(tobs)

@app.route("/api/v1.0/<start_date>")
def data_start_date(start_date):
    """Return a list of min, avg and max tobs for an specific start date"""
    session = Session(engine)

    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                filter(Measurement.date >= start_date).all()

    session.close()
    
    # Create a dictionary from the row data and append to a list of start_date_tobs
    start_date_tobs = []
    for min, avg, max in results:
        start_date_tobs_dict = {}
        start_date_tobs_dict["min_temp"] = min
        start_date_tobs_dict["avg_temp"] = avg
        start_date_tobs_dict["max_temp"] = max
        start_date_tobs.append(start_date_tobs_dict) 

    return jsonify(start_date_tobs)

@app.route("/api/v1.0/<start_date>/<end_date>")
def data_start_end_date(start_date, end_date):
    """Return a list of min, avg and max tobs for an specific start and end dates"""
    session = Session(engine)

    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

    session.close()
    
    # Create a dictionary from the row data and append to a list of start_end_date_tobs
    start_end_date_tobs = []
    for min, avg, max in results:
        start_end_date_tobs_dict = {}
        start_end_date_tobs_dict["min_temp"] = min
        start_end_date_tobs_dict["avg_temp"] = avg
        start_end_date_tobs_dict["max_temp"] = max
        start_end_date_tobs.append(start_end_date_tobs_dict) 

    return jsonify(start_end_date_tobs)

if __name__ == "__main__":
    app.run(debug=True)