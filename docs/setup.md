# FX Market Data Platform - Setup

Ce document explique comment configurer et lancer le projet sur votre environnement local.

# 1. Prérequis

Avant de commencer, assurez-vous d'avoir :
- Python 3.10 +
- Un compte Snowflake
- Git

## 2. Installation

Cloner le dépôt et installer les dépendances :

```
git clone <url_projet_github>
cd fx-market-data-platform
python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

## 3. Configuration des connexions

### 3.1 dbt / Snowflake

Créer un fichier ~/.dbt/profiles.yml

```
fx_market:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <ACCOUNT>
      user: <USER>
      password: <PASSWORD>
      role: <ROLE>
      database: <DATABASE>
      warehouse: <WAREHOUSE>
      schema: <SCHEMA>
```

Remplacer les valeurs <...> par vos informations Snowflake.

### 3.2 Streamlit / Snowflake

Créer le fichier streamlit_app/.streamlit/secrets.toml :
```
[snowflake]
account = "xxx"
user = "xxx"
password = "xxx"
warehouse = "xxx"
database = "xxx"
schema = "xxx"
```

Ce fichier est ignoré par Git pour des raisons de sécurité.

## 4. Lancer le pipeline Airflow

1. Lancer Airflow : ```airflow standalone```
2. Accéder à l'UI d'Airflow : http://localhost:8080
3. Déclencher le DAG fx_rates_dag
4. Vérifier les logs et le succès du pipeline

## 5. Lancer l'application Streamlit

Depuis le dossier streamlit_app :
```
streamlit run app.py
```
- Sélectionner la devise (USD, GBP, JPY)
- Visualiser les indicateurs liés aux taux

## 6. Notes 

Les fichiers de connexion à Snowflake ne doivent jamais être partagés sur GitHub.