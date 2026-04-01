import duckdb
import os

BRONZE_DIRECTORY = "/app/data/bronze"

def run_silver():

	con = duckdb.connect("/app/data/silver/f1_projects.db")

	con.execute("""DROP TABLE IF EXISTS telemetry;""")

	for file in os.listdir(BRONZE_DIRECTORY):

		if "telemetry" in file:


			pilot_name = file.split("_")[0].upper()

			con.execute("""CREATE TABLE IF NOT EXISTS telemetry
			   (pilot_name VARCHAR,
			  date TIMESTAMP,
				session_time VARCHAR);""")

			con.execute(f"""INSERT INTO telemetry
			SELECT '{pilot_name}' AS pilot_name,
			Date,
			SessionTime AS session_time
			FROM read_json_auto('{os.path.join(BRONZE_DIRECTORY, file)}');""")

			count = con.execute("SELECT count(*), Pilot_name FROM telemetry GROUP BY Pilot_name").fetchall()
			print(f"📊 Lignes par pilote : {count}")

			# Affiche les 10 premières lignes de la table telemetry
			print("👀 Aperçu de la table Silver :")
			con.execute("SELECT * FROM telemetry LIMIT 10").show()