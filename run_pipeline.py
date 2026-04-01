from python.bronze.get_raw_data import run_bronze
from python.silver.transform_data import run_silver

if __name__ == "__main__":
    print("🚀 Début du Pipeline")
    run_bronze()
    run_silver()
    print("✅ Fin du Pipeline")