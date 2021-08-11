# stats

## Retention

To run we need:
* Postgres DB with synapse's schema
* MySQL with retention/schema.sql

When testing locally we assume you've got a Postgres Docker container running on the `synapse` network with an alias of `synapse-db`. To populate the DB it is probably sensible to run synapse against it.

To create a MySQL container (from the retention sub-directory):
```
docker run --name stats-db -e MYSQL_RANDOM_ROOT_PASSWORD=yes -e MYSQL_DATABASE=stats -e MYSQL_USER=stats -e MYSQL_PASSWORD=stats -v "$(pwd)/schema.sql:/docker-entrypoint-initdb.d/schema.sql" --network synapse --network-alias stats-db -d mysql:5
```

Create a .env containing (for example):
```
SYNAPSE_DB_HOST=synapse-db
SYNAPSE_DB_USERNAME=postgres
SYNAPSE_DB_PASSWORD=
SYNAPSE_DB_DATABASE=synapse
SYNAPSE_DB_OPTIONS=
STATS_DB_HOST=stats-db
STATS_DB_USERNAME=stats
STATS_DB_PASSWORD=stats
STATS_DB_DATABASE=stats
```

To then run the cohort analysis script:
```
docker build -t cohort_analysis .
docker run -it --env-file .env --network synapse --network-alias cohort-analysis --rm cohort_analysis <ARGS>
```

## Unit tests

**DO NOT** run the unit tests against a real Synapse database. The unit tests will create their own tables, deleting existing ones first if necessary.
