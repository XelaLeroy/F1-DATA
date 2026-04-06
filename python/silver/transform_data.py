import duckdb
import os

BRONZE_DIRECTORY = "/app/data/bronze"

def run_silver():

    # Connexion à DuckDB en mémoire
	con = duckdb.connect(database=':memory:')


	con.execute("""DROP TABLE IF EXISTS telemetry;""")


	# CRÉATION DE LA TABLE TELEMETRY AVEC NOMMAGE DE COLONNES ET TYPAGES SUIVANT LE FICHIER JSON
	con.execute("""CREATE TABLE IF NOT EXISTS telemetry
			   (pilot_name VARCHAR,
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

	for file in os.listdir(BRONZE_DIRECTORY):

		if "telemetry" in file:


			pilot_name = file.split("_")[0].upper()

			con.execute(f"""INSERT INTO telemetry
			SELECT '{pilot_name}' AS pilot_name,
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


	#  Ecrit en .parquet car plus simple à lire, à requeter et à visualiser via DuckDB
	con.execute("""COPY
    (SELECT * FROM telemetry)
    TO '/app/data/silver/f1_data.parquet'
    (FORMAT parquet);""")


	count = con.execute("SELECT count(*), Pilot_name FROM telemetry GROUP BY Pilot_name").fetchall()
	print(f"📊 Lignes par pilote : {count}")

	con.close()

