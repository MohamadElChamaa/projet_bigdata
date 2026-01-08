Analyse de la criminalité vs prix de l'immobilier par département

Description du projet

Ce projet étudie la relation entre les prix des logements et la criminalité au niveau départemental en France.  
L’objectif est de créer une base de données organisée à partir de sources OpenData, et de réaliser des transformations et analyses avec PySpark.

Deux sources de données sont utilisées :

1. Valeurs Foncières (DVF) : prix des logements et surface réelle bâtie  
   Source : [Data.gouv.fr - DVF](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres/)  
   Stockée dans MongoDB (base non relationnelle).

2. Criminalité départementale : taux d’homicides par département  
   Source : [Data.gouv.fr - Base statistique de la délinquance](https://www.data.gouv.fr/datasets/bases-statistiques-communale-departementale-et-regionale-de-la-delinquance-enregistree-par-la-police-et-la-gendarmerie-nationales/)  
   Stockée dans PostgreSQL (base relationnelle).


Structure du projet

├── docker-compose.yml # Déploiement Spark, MongoDB et PostgreSQL
├── app/
│ └── job.py # Script PySpark pour import, transformation et stockage
└── data/
├── ValeursFoncieres-2025-S1.txt
└── donnee-dep-data.gouv-2024-geographie2025-produit-le2025-06-04.csv

Installation et exécution

1. Démarrer les conteneurs Docker:  
```bash
docker-compose up -d
```
2. Installer le driver PostgreSQL pour Spark
```bash
wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar -P /opt/spark/jars/
```
3. Lancer le script PySpark
```bash
docker exec -it spark bash
```
Puis exécuter :
```bash
/opt/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 /app/job.py
```


Fonctionnement du script job.py: 

Création de la session Spark avec les packages MongoDB et PostgreSQL.

Chargement et nettoyage des données DVF :

Lecture CSV

Nettoyage et conversion en numérique

Calcul du prix au m² (prix_m2)

Agrégation par département

Chargement des données criminalité :

Lecture CSV

Filtrage sur l’indicateur 'Homicides'

Sélection des colonnes code_departement et taux_pour_mille

Stockage des données :

DVF → MongoDB (dvf collection)

Criminalité → PostgreSQL (criminalite table)

Jointure départementale DVF × Criminalité

Affichage du résultat final :

+----------------+------------------+---------------+
|code_departement|           prix_m2|taux_pour_mille|
+----------------+------------------+---------------+
|              11|2532.95           |0.0054344      |
|              10|2741.80           |0.0194231      |
|              12|2321.35           |0.0035881      |
|              15|1693.22           |0.0137015      |
| ...            | ...               | ...           |
+----------------+------------------+---------------+

Outils utilisés

PySpark : traitement et transformation des données

Pandas : manipulation locale des CSV

MongoDB : stockage non relationnel pour DVF

PostgreSQL : stockage relationnel pour criminalité

Docker Compose : orchestration des conteneurs

wget : téléchargement du driver PostgreSQL