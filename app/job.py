# job.py
from pyspark.sql import SparkSession
import pandas as pd

# Spark session avec MongoDB + PostgreSQL packages
spark = SparkSession.builder \
    .appName("CriminaliteVsImmobilier") \
    .config("spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,"
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017/dvf.prix") \
    .getOrCreate()

# Charger DVF
dvf_file = "/data/ValeursFoncieres-2025-S1.txt"
dvf = pd.read_csv(dvf_file, sep='|', low_memory=False)
dvf.columns = [c.strip().lower().replace(' ', '_') for c in dvf.columns]
dvf = dvf[['code_departement', 'valeur_fonciere', 'surface_reelle_bati']]
dvf['valeur_fonciere'] = dvf['valeur_fonciere'].astype(str).str.replace(' ', '').str.replace(',', '.')
dvf['surface_reelle_bati'] = dvf['surface_reelle_bati'].astype(str).str.replace(' ', '').str.replace(',', '.')
dvf['valeur_fonciere'] = pd.to_numeric(dvf['valeur_fonciere'], errors='coerce')
dvf['surface_reelle_bati'] = pd.to_numeric(dvf['surface_reelle_bati'], errors='coerce')
dvf = dvf.dropna()
dvf = dvf[(dvf['valeur_fonciere']>0) & (dvf['surface_reelle_bati']>0)]
dvf['prix_m2'] = dvf['valeur_fonciere'] / dvf['surface_reelle_bati']
dvf = dvf.groupby('code_departement')['prix_m2'].mean().reset_index()
dvf = dvf.head(50)

dvf_spark = spark.createDataFrame(dvf)
print(" DVF chargé :")
dvf_spark.show()

#Charger criminalité
crime_file = "/data/donnee-dep-data.gouv-2024-geographie2025-produit-le2025-06-04.csv"
crime = pd.read_csv(crime_file, sep=';', low_memory=False)
crime.columns = [c.strip().lower().replace(' ', '_') for c in crime.columns]
crime = crime[crime['indicateur']=='Homicides'][['code_departement','taux_pour_mille']]
crime = crime.head(50)
crime_spark = spark.createDataFrame(crime)
print("Criminalité chargé :")
crime_spark.show()

# Stocker DVF dans MongoDB
dvf_spark.write.format("mongodb").mode("overwrite").save()
print("DVF stocké dans MongoDB")

#Stocker criminalité dans PostgreSQL
crime_spark.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/crime_db") \
    .option("dbtable", "criminalite") \
    .option("user", "user") \
    .option("password", "pass") \
    .mode("overwrite") \
    .save()
print("Criminalité stockée dans PostgreSQL")

#Jointure départementale
df_final = dvf_spark.join(crime_spark, on="code_departement", how="inner")
print("Jointure effectuée :")
df_final.show()



spark.stop()
