#%%
# Import Dependencies
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import json
import flask
from flask import Flask, jsonify
#%%
engine = create_engine('sqlite:///Resources/hawaii.sqlite')
#%%
app = Flask(__name__)
#%%
@app.route("/")
def welcome():
    return (
        f"Welcome to Surf's Up Hawaii Climate API!<br/>"
        f"Choose your destination:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/end<br/>"
    )
#%%
@app.route('/api/v1.0/precipitation')
def prcp():
    conn = engine.connect()
    query = '''
        SELECT
            date,
            AVG(prcp) as avg_prcp
        FROM
            measurement
        WHERE
            date >= (SELECT DATE(MAX(date), '-1 year') FROM measurement)
        GROUP BY
            date
        ORDER BY
            date
'''
    prcp_df = pd.read_sql(query, conn)
    prcp_df['date'] = pd.to_datetime(prcp_df['date'])
    prcp_df.sort_values('date')
    prcp_json = prcp_df.to_json(orient = 'records', date_format = 'iso')
    conn.close()
    return prcp_json
#%%
@app.route('/api/v1.0/stations')
def stn():
    conn = engine.connect()
    query = '''
        SELECT
            s.station AS station_code,
            s.name AS station_name
        FROM
            measurement m
        INNER JOIN station s
        ON m.station = s.station
        GROUP BY
            s.station,
            s.name
    '''
    active_stations_df = pd.read_sql(query, conn)
    active_stations_json = active_stations_df.to_json(orient = 'records')
    conn.close()
    return active_stations_json
#%%
@app.route('/api/v1.0/tobs')
def act():
    conn = engine.connect()
    query = '''
        SELECT
            s.station AS station_code,
            s.name AS station_name,
            COUNT(*) AS station_count
        FROM
            measurement m
        INNER JOIN station s
        ON m.station = s.station
        GROUP BY
            s.station,
            s.name
        ORDER BY
            station_count DESC
    '''
    active_stations_df = pd.read_sql(query, conn)
    active_stations_df.sort_values('station_count', ascending=False, inplace=True)
    most_active_station = active_stations_df['station_code'].values[0]
    query = f'''
        SELECT
            tobs
        FROM
            measurement
        WHERE
            station = '{most_active_station}'
            AND
            date >= (SELECT DATE(MAX(date), '-1 year') FROM measurement)
    '''
    act_tobs_df = pd.read_sql(query, conn)
    act_tobs_json = act_tobs_df.to_json(orient = 'records')
    conn.close()
    return act_tobs_json
#%%
@app.route('/api/v1.0/<start>')
def date_stat_start(start):
    conn = engine.connect()
    query = f'''
        SELECT
            MIN(tobs) AS TMIN,
            MAX(tobs) AS TMAX,
            AVG(tobs) AS TAVG
        FROM
            measurement
        WHERE
            date >= '{start}'
    '''
    start_date_stats_df = pd.read_sql(query, conn)
    start_date_stats_json = start_date_stats_df.to_json(orient = 'records')
    conn.close()
    return start_date_stats_json
#%%
@app.route('/api/v1.0/<start>/<end>')
def date_stat_end(start, end):
    conn = engine.connect()
    query = f'''
        SELECT
            MIN(tobs) AS TMIN,
            MAX(tobs) AS TMAX,
            AVG(tobs) AS TAVG
        FROM
            measurement
        WHERE
            date BETWEEN '{start}' AND '{end}'
    '''
    end_date_stats_df = pd.read_sql(query, conn)
    end_date_stats_json = end_date_stats_df.to_json(orient = 'records')
    conn.close()
    return end_date_stats_json
#%%
if __name__ == '__main__':
    app.run(debug=False)