import duckdb
import os
from dotenv import load_dotenv

BRONZE_DIRECTORY = "/app/data/bronze"

load_dotenv()

def run_silver():

    # Connexion à DuckDB en mémoire
	con = duckdb.connect(database='md:')

	con.execute("""DROP TABLE IF EXISTS telemetry;""")

	# CRÉATION DE LA TABLE TELEMETRY AVEC NOMMAGE DE COLONNES ET TYPAGES SUIVANT LE FICHIER JSON
	con.execute("""CREATE TABLE IF NOT EXISTS telemetry
			   (pilot_name VARCHAR,
			 	speed FLOAT,
				time DOUBLE,
			    driver_ahead STRING,
			    drs BOOLEAN,
			    throttle BOOLEAN,
			    status VARCHAR,
			    brake BOOLEAN,
			    position_x DOUBLE,
			    position_y DOUBLE,
			    position_z DOUBLE,
			    date TIMESTAMP,
				session_time VARCHAR
			    );""")

	con.execute("""CREATE TABLE IF NOT EXISTS fastest_lap
			   (pilot_name VARCHAR,
			 	speed FLOAT,
				time DOUBLE,
			    driver_ahead STRING,
			    drs BOOLEAN,
			    throttle BOOLEAN,
			    status VARCHAR,
			    brake BOOLEAN,
			    position_x DOUBLE,
			    position_y DOUBLE,
			    position_z DOUBLE,
			    date TIMESTAMP,
				session_time VARCHAR
			    );""")

	con.execute("""CREATE TABLE IF NOT EXISTS results
			   (pilot_name VARCHAR,
			 	position INTEGER,
				points INTEGER,
			    grid_position INTEGER,
			    laps INTEGER,
			    status VARCHAR,
			 	nationality VARCHAR,
			    team VARCHAR
			    );""")

	for file in os.listdir(BRONZE_DIRECTORY):

		if "telemetry" in file:


			pilot_name = file.split("_")[0].upper()

			con.execute(f"""INSERT INTO telemetry
			SELECT '{pilot_name}' AS pilot_name,
			Speed AS speed,
			Time AS time,
			DriverAhead AS driver_ahead,
			DRS AS drs,
			Throttle AS throttle,
			Status AS status,
			Brake AS brake,
			X AS position_x,
			Y AS position_y,
			Z AS position_z,
			Date,
			SessionTime AS session_time
			FROM read_json_auto('{os.path.join(BRONZE_DIRECTORY, file)}');""")

		if "results" in file:

			con.execute(f"""INSERT INTO results
			SELECT
				r.Driver->>'familyName'   AS pilot_name,
				r.position                AS position,
				r.points                  AS points,
				r.grid                    AS grid_position,
				r.laps                    AS laps,
				r.status                  AS status,
				r.Driver->>'nationality'  AS nationality,
				r.Constructor->>'name'    AS team
			FROM read_json_auto('{os.path.join(BRONZE_DIRECTORY, file)}') AS race,
    UNNEST(race.Results) AS t(r);""")

	con.close()

