import logging
from googleads import adwords

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from sqlalchemy import create_engine
import sys



logging.basicConfig(level=logging.INFO)
logging.getLogger('suds.transport').setLevel(logging.DEBUG)


PAGE_SIZE = 100

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION =  '/Users/vasista/Downloads/frilly-ga-2362b9aa7723.json'
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


def main(client):
    # Initialize appropriate service.
    campaign_service = client.GetService('CampaignService', version='v201802')

    # Construct selector and get all campaigns.
    offset = 0
    selector = {
        'fields': ['Id', 'Name', 'Status'],
        'paging': {
            'startIndex': str(offset),
            'numberResults': str(PAGE_SIZE)
        }
    }

    more_pages = True
    while more_pages:
        page = campaign_service.get(selector)

        # Display results.
        if 'entries' in page:
            for campaign in page['entries']:
                print ('Campaign with id "%s", name "%s", and status "%s" was '
                       'found.' % (campaign['id'], campaign['name'],
                                   campaign['status']))
        else:
            print('No campaigns were found.')
        offset += PAGE_SIZE
        selector['paging']['startIndex'] = str(offset)
        more_pages = offset < int(page['totalNumEntries'])


if __name__ == '__main__':
    adwords_client = adwords.AdWordsClient.LoadFromStorage("/Users/vpolal/IdeaProjects/gaload/googleleads.yaml")

    main(adwords_client)