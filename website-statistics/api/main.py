from fastapi import FastAPI

import json
import datetime as dt
import pandas as pd


app = FastAPI()

data = []
with open("/data/listen_events", "r") as f:
    for line in f:
        datum = json.loads(line)
        datum["date"] = dt.datetime.fromtimestamp(datum.pop("ts") / 1000.0).date()
        data.append(datum)


df = pd.DataFrame(data)


@app.get("/events/last30")
def get_last30_events():
    today = pd.Timestamp.today()

    return df[
        df["date"].between(today - pd.Timedelta(days=30), today, inclusive="left")
    ].to_dict(orient="records")


@app.get("/events")
def get_day_events(start_date: dt.date, end_date: dt.date):
    return df[df["date"].between(start_date, end_date, inclusive="left")].to_dict(
        orient="records"
    )
