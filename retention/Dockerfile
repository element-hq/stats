FROM python:3.9

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# ENV SYNAPSE_DB_HOST=,
#     SYNAPSE_DB_USERNAME=,
#     SYNAPSE_DB_PASSWORD=,
#     SYNAPSE_DB_DATABASE=,
#     SYNAPSE_DB_OPTIONS=,
#     STATS_DB_HOST=,
#     STATS_DB_USERNAME=,
#     STATS_DB_PASSWORD=,
#     STATS_DB_DATABASE=

ENTRYPOINT [ "python3", "./cohort_analysis.py" ]
