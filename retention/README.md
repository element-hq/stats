# To run tests locally
Create the local postgres instance.

```$ docker run --name postgresql-container -p 5432:5432 -e POSTGRES_USER=test -e POSTGRES_PASSWORD=somePassword -d postgres```

Run tests with the appropriate environment variables.

```SYNAPSE_DB_HOST=localhost SYNAPSE_DB_USERNAME=test SYNAPSE_DB_PASSWORD=somePassword SYNAPSE_DB_DATABASE=postgres SYNAPSE_DB_OPTIONS= STATS_DB_HOST= STATS_DB_USERNAME= STATS_DB_PASSWORD= STATS_DB_DATABASE= python3 test/test_cohort_analysis.py```
