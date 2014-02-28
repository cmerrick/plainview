#!/bin/bash
sqoop export \
  --connect "jdbc:mysql://127.0.0.1:5001/sample_data" \
  --username "replication-user" \
  --password "password" \
  --table "users_raw" \
  --export-dir "example/raw/372/"
