### This script queries data from Google BigQuery and uploads to Mixpanel
### Can be used with cronjob on Linux to run daily
### All data in the script are mock data

import requests
import pandas as pd
from datetime import *

yesterday = date.today() - timedelta(1)

# Get yesterday's event from from Google BigQuery
QUERY = """
    SELECT
        insert_id, distinct_id, event_property_1, event_property_2,
        created_time AS time
    FROM
       `event_table`
    WHERE
        DATE(created_time) = DATE('{0}')
""".format(yesterday)

df = pd.read_gbq(QUERY, project_id='testproject', dialect='standard')
df.fillna('', inplace=True)
df['created_time'] = df['created_time'].astype(str)

df_dict = df.to_dict(orient='records')
data = []

# process data from dictionary
for rows in df_dict:
    event = {}
    event["event"] = "test_event"
    event["properties"] = rows
    event["properties"]["$insert_id"] = event["properties"].pop("insert_id")
    event["properties"]["time"] = int(event["properties"].pop("time")) # Mixpanel time must be int
    event["properties"]["event_property_1"] = float(event["properties"].pop("event_property_1")) # Convert string to float
    data.append(event)

# Mixpanel accepts 2000 events at once
# split data to chunks of 1800
n = 1800
final = [data[i * n:(i + 1) * n] for i in range((len(data) + n - 1) // n )]

# MP API
url = "https://api.mixpanel.com/import"
querystring = {"strict":"1","project_id":"xxxxxxx"} #project_id here

for batches in final: # upload batches of 1800 events
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.request("POST", url, auth=('authenticate_service_account', 'authenticate_password'), json=batches, headers=headers, params=querystring)
    print(response.text)
