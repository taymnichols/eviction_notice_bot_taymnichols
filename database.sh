#!/bin/bash

# Specify the paths and names
database_file="evictions.db"
table_name="scheduled_evictions"
csv_file="scraper/eviction_notices.csv"

# Create SQLite database from CSV
sqlite-utils insert $database_file $table_name --csv $csv_file

# Serve the SQLite database using Datasette
datasette $database_file
