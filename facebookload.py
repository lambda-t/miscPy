import os
import json
import urllib
import pprint


from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.adcreative import AdCreative
from facebookads.adobjects.ad import Ad


from facebookads.api import FacebookAdsApi
from facebookads import adobjects
import requests as r
import json



#APP ID: 216523165752115
#App Secret:83d3bbd1111701e624685c56915e7f59
#App Token	216523165752115|PwXuxY-VMILR0UMgqTXEIt0S6_8
#User Token	EAADE7TphWzMBAEwjGLP1ZBiqycUqaA7V9httYnu7lkLvVHINPZA0IGcDDUE0pEhnCsGfTTLISkH1u3tYOhqpVS7pX

"""
app_id = '216523165752115'
app_secret = '83d3bbd1111701e624685c56915e7f59'
app_token =	'216523165752115|PwXuxY-VMILR0UMgqTXEIt0S6_8'
user_token='EAADE7TphWzMBAEwjGLP1ZBiqycUqaA7V9httYnu7lkLvVHINPZA0IGcDDUE0pEhnCsGfTTLISkH1u3tYOhqpVS7pX'
access_token = 'EAADE7TphWzMBAKZBZBNWyKPDiWxYgb7HmbeLqaR0WxjzqcZBIlE29CJUNTCUGZAeGmFvnPDqGUOnJvk4DogM2Hw3FQhGeMmIRgDNUC8iiLzsEIZCa0RJgVLIGS9ilefgc6iPZB7razH3lORkdAGLvE4jh8ZAExYYYOKyeU2ZAxixdHxn9Fw7YhU2ntqQxImvPmUZD'
"""

app_id = "146054626071110"
app_secret = "fb66e45bd80f57c619a209e77b09a905"
user_token='EAAF7pPE6azkBABq1SjxBDZALNCyua8KncmEMDeZBHshZA2ySIZCKtN6jfBunBkGbYPS1LwrDxC5ETccT6Tcs' \
           'FDYX7ZA1m9QkSUgH3tEJpJPT6Vj9I5rBqe7TlPT3qCffGcScJm3nI6ZAzNyM9nZB61vCAmOV8IZBZAxm71DwnnVibDgZAdfne3wRpgFuHMcwx5eKoZD'
access_token="EAACE1f4a0kYBAPlUxz5AbMmwzQapkJ0DkkGLPEjDBB2ZCwRxZCAZAN84MGwEOTCgzJBgAEMEMKHFQzuBe5SZAbXjHA39I9ztYMDSlks9PxQ2mYKh23IiZBm2fBEoPnJ4YgnDCCuRltrqA2edePRYvKukhoFJUzio9wa8k2BXYqADmcKg0jI25ZADv2ZA9m5OZB4ZD"
app_token ="417423328701241|hyOJvm8h5JpFXMyOiMQQZhOBwFs"

FacebookAdsApi.init(app_id,app_secret,access_token)


account = AdAccount('act_693598040844198')
ad = Ad(fbid='act_693598040844198')
creatives = ad.get_ad_creatives(fields=[AdCreative.Field.id,
                                        AdCreative.Field.object_story_spec,
                                         ])




#print(account.get_activities())
#"event_time": "2018-03-10T01:05:58+0000",
#"event_type": "update_ad_run_status"
header = "access_token=" + access_token

#url = "https://graph.facebook.com/v2.10/act_693598040844198/campaigns?fields=name,status,insights{reach,impressions,clicks}"

#res = r.get(url,headers=header)


#i = AdAccount(aid).get_insights(fields=fields, async=True)
from facebookads.adobjects.campaign import Campaign
from facebookads.adobjects.adsinsights import AdsInsights  as Insights
campaign = Campaign('23842767635360263')

params = {

    'time_range':{'since':'2018-02-01','until':'2018-03-01'}

}

insights = account.get_insights(params=params)

print (insights)

#print(campaign.Field.name.Impressions)
#print(campaign.get_ads(fields=["name","status","insights{reach,impressions,clicks}"]))
#print(campaign[adobjects.campaign])




#insights = campaign.get_insights(params=params)
#print(insights)

#print(account.remote_read(fields=[AdAccount.Field.end_advertiser_name]))
#adsets = account.get_ad_sets(fields=[AdSet.Field.name])


"""
Date
Campaign
Device from Google
Impressionsss
Clicksss
Spend
Conversionsss
Revenuesss
Actions
Reach
Frequency
People Taking Action
Post Engagement
Post Shares
Post Comments
Post Reactions
Page Likes
Page Engagement
Ad
Adsets  
for adset in adsets:
print(adset[AdSet.Field.name])

#EAACE1f4a0kYBAGRFxjjLLNg5OkJ3zkv3JBrF9YicTXH3weI6z0ZC1ZBNRHo5aKrXlxn9VArrd9mDwfCbx8EhTaGZCoopGQHeJ3TYqoG4qJvTbHlpfTWlSMxeVr6gZCs6nt59R6ZBrJvgcazbQZB7ob1RZBbKBDI7WwZC1noHnZCArOx9ZAjaIVJQ2ZBMakK1yKiseQZD
# get Facebook access token from environment variable
ACCESS_TOKEN = 'EAACEdEose0cBAJMunhrAbpUdtCFImi0CzZCKqBl4VFeVZAXCVxncnglhRgoQuJzzygZClaBmR7Idb9YbjB91gZB59ZBozsNjLH9yHL5h5CdOZCOYAgfRWqsbUxWaFkXatJb6Qjb9CZC5692JIdHtEy4pvQvgG7RdE6eQuWGfwwVihmoXYah1UdauZCwH9ePeUiUZD'
graph = facebook.GraphAPI(access_token=ACCESS_TOKEN, version = "2.7")

events = graph.request('thefrilly?fields=id,name')



#{'id': '181562708852887', 'name': 'frilly'}

"""


































#EAACEdEose0cBAAwOMQUkGYYxuZAbaWL4QZBk03xQMyaAZC4x3zewMxZC4QlEwfAn0zA7h5FgbxLrk26A55kWmZC7r0wgO0LmTW6NUxDDwZBDi4NMHY0SrCLKEtwEcs4ptZBVKDs0P4xKBzmNqCL30IQZCVXDTEda0VAlDThQb87zch7bTqwzKAGN0aJNPklZACdiPEvFjzJuI48dQI1e3QNJQkoZCa4kmCvsQZD


