import fastf1
import pandas as pd
import os
from pathlist import Path
from fastf1.ergast import Ergast



BASE_DIR = Path(__file__).resolve().parent.parent

BRONZE_DIR = BASE_DIR / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

CACHE_DIR = "/app/f1_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

fastf1.Cache.enable_cache(CACHE_DIR)


def run_bronze():


    ergast = Ergast()

    pilotes_list = ["LEC","ANT"]

# Get all the circuits.
# Transform in a DataFrame Pandas
# Transform the df to json and choose the right path to load the output file

    circuits = ergast.get_circuits(season=2022, result_type='raw')

    df_temp_circuits = pd.DataFrame(circuits)
    df_temp_circuits.to_json(
        os.path.join(BRONZE_DIR, 'circuits.json'),
        orient="records",
        date_format="iso",
        indent=4
    )

# Get the results of the second GP of 2026.
# Transform in a DataFrame Pandas
# Transform the df to json and choose the right path to load the output file

    gp2 = ergast.get_race_results(2026, 1, result_type='raw')

    df_temp= pd.DataFrame(gp2)
    df_temp.to_json(
        os.path.join(BRONZE_DIR, 'results.json'),
        orient="records",
        date_format="iso",
        indent=4
        )

    # Get all the car data for each driver

    # Get specific event (GP2)
    session = fastf1.get_session(2026, 2, 'R')
    session.load()

    for pilot in pilotes_list:


    # 2. Choisir un pilote (ex: Leclerc 'LEC') and his rival (Antonelli)
        fastest_lap = session.laps.pick_drivers(pilot)

        # 3. Récupérer la télémétrie de ce tour précis
        # La méthode 'get_telemetry()' renvoie l'objet dont parle ta doc
        tel= fastest_lap.get_telemetry()


        # 4. Nettoyage rapide pour ton Bronze/Silver
        # On convertit les Timedelta en secondes (flottants) car le JSON déteste les Timedelta
        tel['Time'] = tel['Time'].dt.total_seconds()
        tel.to_json(
            os.path.join(BRONZE_DIR, f'{pilot}_telemetry.json'),
            orient="records",
            date_format="iso",
            indent=4
        )


        # Get drivers
        laps = session.laps
        drivers = session.drivers
        # Transform drivers numbers into a 3 letters Abbreviation like "VER"
        drivers = [session.get_driver(driver)["Abbreviation"] for driver in drivers]

        # We get the stints (relais) and get the needed column on the laps object
        stints = laps[["Driver", "Stint", "Compound", "LapNumber"]]
        # Group les coureurs par relais et type de pneus
        stints = stints.groupby(["Driver", "Stint", "Compound"])
        # Count the laps in each group by, the group by columns stayed
        stints = stints.count().reset_index()
        stints = stints.rename(columns={"LapNumber": "StintLength"})

        stints.to_json(
            os.path.join(BRONZE_DIR, 'stints.json'),
            orient="records"
        )

