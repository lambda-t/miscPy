"""
API Key:
cyxzXYPu0p1PgJDz8FF9wwaIoiksWhjy4WnstqKBwdcOls2d1X5EdXSHZr4KgQ9k

Secret Key:  To ensure safety, API Secret Key will only be displayed at the time of being created. And if the key is lost, you should delete the API and set up a new one.
bP0c3tUsBlYQQ98pbBuUtkqXUjhT7VrQcLmCBWxOpJZpE2dE6kI6dhOAFZMSAuXT
 Store your Secret Key somewhere safe. It will not be shown again.

"""

import requests as r
import pandas as pd

API_KEY = "cyxzXYPu0p1PgJDz8FF9wwaIoiksWhjy4WnstqKBwdcOls2d1X5EdXSHZr4KgQ9k"

header =  {'Accept': 'application/json',
           'User-Agent': 'binance/python',
           'X-MBX-APIKEY': API_KEY}

res = r.get("https://api.binance.com/api/v1/depth",headers=header,params={"symbol":"BNBBTC"})

df = pd.DataFrame(res.json())
print(df)