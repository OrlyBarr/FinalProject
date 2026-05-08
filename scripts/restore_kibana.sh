#!/bin/bash
# Restore Kibana dashboards from latest backup
LATEST=$(ls -t /home/local_admin/finalproject/backups/kibana_full_*.ndjson 2>/dev/null | head -1)
if [ -z "$LATEST" ]; then echo 'No backup found'; exit 1; fi
echo "Restoring from: $LATEST"
curl -sm 120 -X POST 'http://localhost:5601/api/saved_objects/_import?overwrite=true'   -H 'kbn-xsrf: true' --form file=@$LATEST | python3 -m json.tool
