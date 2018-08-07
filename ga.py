from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from sqlalchemy import create_engine
import sys
import logging




logging.basicConfig(level=1)

logger = logging.getLogger("ga_ingest")



if len(sys.argv) < 9:
    print("Insuffient arguments, provide <viewid> <key_file> <table_name> <error_table_name> <start_date> <end_date> <operation_on_table>(option are replace or append) <sanity_check_number>" )
    sys.exit(1)


view_id = sys.argv[1]
key_file = sys.argv[2]
table_name = sys.argv[3]
error_table_name = sys.argv[4]
start_date = sys.argv[5]
end_date = sys.argv[6]
operation = sys.argv[7]
sanity_number = int(sys.argv[8])




SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION =  key_file #'/Users/vasista/Downloads/frilly-ga-2362b9aa7723.json'
VIEW_ID = view_id
#'133436500'



#Metrics: visits, transactions (Goal 8), bounces, Session Duration



#Dimensions: total, device category, channel, and landing page


def initialize_analyticsreporting():
    """Initializes an Analytics Reporting API V4 service object.

    Returns:
      An authorized Analytics Reporting API V4 service object.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics


def get_report(analytics):
    """Queries the Analytics Reporting API V4.

    Args:
      analytics: An authorized Analytics Reporting API V4 service object.
    Returns:
      The Analytics Reporting API V4 response.
    """
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'dateRanges': [{"startDate": start_date, "endDate": end_date}],
                    #'metrics': [{'expression': 'ga:sessions'}],
                    'metrics': [{'expression':'ga:sessions'},{'expression': 'ga:visits'}, {'expression':'ga:transactions'}, {'expression':'ga:bounces'}, {'expression':'ga:sessionDuration'},
                                ],

                    #'dimensions': [{'name': 'ga:country'}]
                    'dimensions': [{'name':'ga:deviceCategory'}, {'name':'ga:medium'},{'name':'ga:landingPagePath'},{'name':'ga:date'},
                                   {'name':'ga:source'},{'name':'ga:adDistributionNetwork'},{'name':'ga:channelGrouping'}],

                    "pageToken": "0",
                    "pageSize": 10000,


                },]
        }
    ).execute()


def print_response(response):
    """Parses and prints the Analytics Reporting API V4 response.

    Args:
      response: An Analytics Reporting API V4 response.
    """


    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])


        l =[]
        n = 0
        for row in report.get('data', {}).get('rows', []):
            d = {}
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])



            for header, dimension in zip(dimensionHeaders, dimensions):
                #print(header + ': ' + dimension)
                d[header] = dimension



            for i, values in enumerate(dateRangeValues):
                #print('Date range: ' + str(i))
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    #print(metricHeader.get('name') + ': ' + value)
                    d[metricHeader.get('name')] = value
            l.append(d)
            n = n +1
    #print("n",n)
    return l






def main():
    dbname = "analytics"
    host = "frillycluster.cydlhimdxe2x.us-west-1.redshift.amazonaws.com"
    port = "5439"
    user = "admin"
    password = "Frilly2017"
    conn = create_engine('postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + dbname)

    conn.execute('drop table if exists ' + table_name)


    analytics = initialize_analyticsreporting()

    logger.info("Pulling data from GA")

    response = get_report(analytics)
    res = pd.DataFrame(print_response(response))
    res.rename(columns={'ga:sessions': 'sessions','ga:visits':'visits','ga:transactions':'transactions','ga:bounces':'bounces','sessionDuration':'sessionDuration'
        ,'ga:deviceCategory':'category','ga:medium':'medium','ga:landingPagePath':'landingpath','ga:date':'date'}, inplace=True)
    #print(res)
    su = sum(res['sessions'].apply(int))

    logger.info("Total Sessions " + str(su))

    logger.info("Inserting data into RedShift")
    if su < int(sanity_number):
        san = sanity_number - su
        per = (100 * san)/ sanity_number
        if abs(per) > 5:

            res.to_sql(error_table_name, conn, index = False, if_exists = 'append')
            logger.info("Data inserted into " + error_table_name  + " because difference was > 5 %")
        else:
            res.to_sql(table_name, conn, index = False, if_exists = operation)
            logger.info("Data inserted into " + table_name)
    else:
        res.to_sql(table_name, conn, index = False, if_exists = operation)
        logger.info("Data inserted into " + table_name)


if __name__ == '__main__':
    main()

