# L'image tourne déjà avec l'utilisateur « airflow » : on n'utilise jamais root ici.
FROM apache/airflow:2.9.3

# Dépendances installées en tant qu'utilisateur airflow, en épinglant airflow pour éviter
# qu'un pip install ne le déplace de version.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" pymongo==4.8.0 pyarrow

ENV AIRFLOW__CORE__LOAD_EXAMPLES=false \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false

COPY --chown=airflow:root dags/ /opt/airflow/dags/
# créé ici (propriétaire airflow) : le volume nommé « data-lake » hérite de ces droits au 1er montage
RUN mkdir -p /opt/airflow/data-lake
