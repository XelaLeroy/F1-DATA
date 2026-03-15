import fastf1
import pandas as pd
import json
from fastf1.ergast import Ergast

ergast = Ergast()

# Get all the circuits.
# Transform in a DataFrame Pandas
# Transform the df to json and choose the right path to load the output file

circuits = ergast.get_circuits(season=2022, result_type='raw')

df_temp_circuits = pd.DataFrame(circuits)
df_temp_circuits.to_json(
	"data/bronze/circuits.json",
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
    "data/bronze/results.json",
    orient="records",
    date_format="iso",
    indent=4
)

# Get all the car data for each driver

# Get specific event (GP2)
session = fastf1.get_session(2026, 2, 'R')
session.load()

# 2. Choisir un pilote (ex: Leclerc 'LEC')
fastest_lap = session.laps.pick_drivers('LEC')

# 3. Récupérer la télémétrie de ce tour précis
# La méthode 'get_telemetry()' renvoie l'objet dont parle ta doc
telemetry = fastest_lap.get_telemetry()

# 4. Nettoyage rapide pour ton Bronze/Silver
# On convertit les Timedelta en secondes (flottants) car le JSON déteste les Timedelta
telemetry['Time'] = telemetry['Time'].dt.total_seconds()
telemetry.to_json(
    "data/bronze/leclerc_telemetry.json",
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
	"data/bronze/stints.json",
	orient="records"
)


# import matplotlib.pyplot as plt
# import pandas as pd
# from timple.timedelta import strftimedelta

# import fastf1
# import fastf1.plotting
# from fastf1.core import Laps


# # Enable Matplotlib patches for plotting timedelta values
# fastf1.plotting.setup_mpl(mpl_timedelta_support=True, color_scheme=None)


# session = fastf1.get_session(2021, 'Spanish Grand Prix', 'Q')
# session.load()

# drivers = pd.unique(session.laps['Driver'])
# print(drivers)

