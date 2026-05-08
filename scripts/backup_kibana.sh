#!/bin/bash
TS=$(date +%Y%m%d_%H%M%S)
mkdir -p /home/local_admin/finalproject/backups
curl -sm 60 -X POST 'http://localhost:5601/api/saved_objects/_export'   -H 'kbn-xsrf: true' -H 'Content-Type: application/json'   -d '{"type":["dashboard","visualization","index-pattern","search"],"includeReferencesDeep":true}'   -o /home/local_admin/finalproject/backups/kibana_full_${TS}.ndjson
# Keep only last 14 backups
ls -t /home/local_admin/finalproject/backups/kibana_full_*.ndjson | tail -n +15 | xargs -r rm
