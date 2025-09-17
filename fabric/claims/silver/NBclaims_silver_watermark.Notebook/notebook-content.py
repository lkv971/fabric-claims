# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b98cd8e8-9a67-459f-9eae-c4311796500b",
# META       "default_lakehouse_name": "LHclaims_silver",
# META       "default_lakehouse_workspace_id": "d6cf1ffb-bc19-4130-bfa1-27701036ae71",
# META       "known_lakehouses": [
# META         {
# META           "id": "b98cd8e8-9a67-459f-9eae-c4311796500b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

now_et   = datetime.now(ZoneInfo("America/New_York"))
now_utc  = now_et.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

folder = "Files/watermarks"
file   = f"{folder}/Watermark.json"

mssparkutils.fs.mkdirs(folder)  
mssparkutils.fs.put(file, json.dumps({"lastModified": now_utc}, indent=2), overwrite=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
