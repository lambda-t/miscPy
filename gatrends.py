from pytrends.request import TrendReq

pytrends = TrendReq(hl='en-US', tz=360)

kw_list = ["Roche Nike"]
pytrends.build_payload(kw_list, cat=0, timeframe='today 5-y', geo='', gprop='')

df = pytrends.interest_over_time(country="United States")

print(df)