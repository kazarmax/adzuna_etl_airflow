#!/usr/bin/env bash
set -e

psql -U airflow -d airflow -f /opt/scripts/create_db.sql

psql -U airflow -d adzuna -f /opt/scripts/create_tables.sql
