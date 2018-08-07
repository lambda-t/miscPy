import os
import pandas as pd
from sqlalchemy import create_engine
import sys
import logging




logging.basicConfig(level=1)

logger = logging.getLogger("fb_ingest")



if len(sys.argv) < 4:
    print("Insuffient arguments, provide <table_name_main_metrics> <table_name_page_metrics> <operation_on_table>(option are replace or append)" )
    sys.exit(1)





table_name_main_metrics = sys.argv[1]
table_name_page_metrics = sys.argv[2]
#start_date = sys.argv[5]
#end_date = sys.argv[6]
operation = sys.argv[3]


def main():
    from datetime import date, timedelta
    yesterday = date.today() - timedelta(1)
    yesterday = yesterday.strftime('%Y-%m-%d')

    dbname = "ganalytic"
    host = "ganalytic.cvqb8lx7aald.us-west-2.redshift.amazonaws.com"
    port = "5439"
    user = "root"
    password = "ShiftRed123."
    conn = create_engine('postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + dbname)

    from facebookads.adobjects.adaccount import AdAccount
    from facebookads.adobjects.adsinsights import AdsInsights  as Insights
    from facebookads.adobjects.adcreative import AdCreative
    from facebookads.adobjects.ad import Ad


    from facebookads.api import FacebookAdsApi

    app_id = "216523165752115"
    app_secret = "83d3bbd1111701e624685c56915e7f59"

    user_token='EAAF7pPE6azkBABq1SjxBDZALNCyua8KncmEMDeZBHshZA2ySIZCKtN6jfBunBkGbYPS1LwrDxC5ETccT6Tcs' \
               'FDYX7ZA1m9QkSUgH3tEJpJPT6Vj9I5rBqe7TlPT3qCffGcScJm3nI6ZAzNyM9nZB61vCAmOV8IZBZAxm71DwnnVibDgZAdfne3wRpgFuHMcwx5eKoZD'


    access_token="EAADE7TphWzMBAI7u4hwkZB7dtNQpZAjnUkJTzCaVeYmJZC1QOXPxZCzn4V9h9vQ1gZC5gad1dGwyPNTZBSjB35Mx1dn6IVQXO" \
                 "7fvh1edSYoMZCmx3gYGNAiLxm0P3n3cEk4fKS7tNgKGF7P8kXMLLZBNyLREGb2xVDZA2bXkavXS8twZDZD"


    app_token ="417423328701241|hyOJvm8h5JpFXMyOiMQQZhOBwFs"

    FacebookAdsApi.init(app_id,app_secret,access_token)


    account = AdAccount("act_693598040844198")
    #account.remote_read(fields=[AdAccount.Field.timezone_name])



    start = "2018-04-01"
    end = "2018-04-01"

    fields = [
        Insights.Field.campaign_name,
        Insights.Field.campaign_id,
        Insights.Field.impressions,
        Insights.Field.reach,
        Insights.Field.actions,
        Insights.Field.spend,
        Insights.Field.ad_id,
        Insights.Field.date_start,
        Insights.Field.date_stop,
        Insights.Field.adset_name,
        Insights.Field.clicks,
        Insights.Field.frequency,
        Insights.Field.unique_actions

        #Insights.Field.People Taking action
        #Insights.Field.Post engemnet
        #Insights.Field.actions


    ]


    params = {
        'time_range': {'since': start, 'until': end},
        'level': 'ad',
        'limit': 10000,
        'breakdowns': ["impression_device","publisher_platform"]
    }
    #insights = account.get_insights(fields=fields, params=params)

    #from facebookads.adobjects.campaign import Campaign
    #campaign = Campaign('2384276763536026')
    insights = account.get_insights(fields=fields, params=params)


    res = [i for i in insights]
    print(res)

    #df = pd.DataFrame(res)
    #print(df["unique_actions"])

    """"
    df_2 = df[['actions','campaign_name','campaign_id','date_start', 'date_stop','impression_device', 'impressions',
               'publisher_platform','reach','spend','ad_id','adset_name','clicks','frequency']]
    df = df.drop(['actions'],axis=1)
    df.to_sql(table_name_main_metrics, conn, index = False, if_exists = operation)
    #print(df)
    #df_2 = df['actions',]
    df_3 = pd.DataFrame()
    #for c in df['actions']:

    for i,r in df_2.iterrows():
        if str(r['actions']) != "nan":
            df_actions = pd.DataFrame(r['actions'])
            df_actions['campaign_id']= r['campaign_id']
            df_actions['date_start']= r['date_start']
            df_actions['date_stop'] = r['date_stop']
            df_actions['impression_device'] = r['impression_device']
            df_actions['impressions'] = r['impressions']
            df_actions['publisher_platform'] = r['publisher_platform']
            df_actions['campaign_name'] = r['publisher_platform']
            df_actions['reach'] = r['reach']
            df_actions['spend'] = r['spend']
            df_actions['ad_id'] = r['ad_id']
            df_actions['adset_name'] = r['adset_name']
            df_actions['clicks'] = r['clicks']
            df_actions['frequency'] = r['frequency']
             #df_actions = df_actions.pivot(columns="action_type",values="value")
            df_3 = pd.concat([df_actions,df_3],axis = 0)
            df_3 = df_3.reset_index()
            df_3 = df_3.drop(['index'],axis=1)
    df_3.to_sql(table_name_page_metrics, conn, index = False, if_exists = operation)
"""


main()

def test():
    df = pd.read_csv("fb_test.cv")
    df_action=(pd.DataFrame())
    for c in df['actions']:
        print(type(c))


