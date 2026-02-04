# FX Market Data Platform

Pipeline data complet pour l'analyse de taux de change (FX), depuis l'ingestion
des données jusqu'à la visualisation.

## Objectif

Collecter quotidiennement des taux de change, les transformer dans un data warehouse
et fournir des indicateurs analytiques accessibles via un dashboard.

## Architecture
- Ingestion : Python + API Frankfurter
- Data Warehouse : Snowflake (Bronze / Silver / Gold)
- Transformations & tests : dbt
- Orchestration : Airflow
- Visualisation : Streamlit

Documentation détaillée :  
- Architecture & choix techniques → `docs/project_documentation.md`  
- Guide de setup et exécution → `docs/setup.md`