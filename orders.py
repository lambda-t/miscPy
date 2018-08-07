from sqlalchemy import create_engine
import sys
import logging
import pandas as pd
import os
import time
import datetime
import logging.handlers


#Enable Logging
LOG_FILENAME = 'log/orders_frilly.log'
# Logger with desired output level
logging.basicConfig(level=1)
log = logging.getLogger("ORDERS_FRILLY")
# Add the log message handler to the logger
handler = logging.handlers.RotatingFileHandler(
    LOG_FILENAME, maxBytes=5000, backupCount=2)
log.addHandler(handler)


if len(sys.argv) < 4:
    print("Insuffient arguments.Provide <inputdir> <backupdir> <table name>")
    sys.exit(1)

inputdir = sys.argv[1]
backupdir = sys.argv[2]
tablename = sys.argv[3]
sheetname = "Sheet1"


def processfile(filename,sheetname,tablename):
    log.info("Creating connection to redshift")
    dbname = "ganalytic"
    host = "ganalytic.cvqb8lx7aald.us-west-2.redshift.amazonaws.com"
    port = "5439"
    user = "root"
    password = "Haha123."
    conn = create_engine('postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + dbname)


    log.info("Loading excel file")
    def load_excel(file_path,sheet_name):
        r = pd.ExcelFile(file_path)
        df = r.parse(sheet_name)
        print(df)
        return df
    log.info("Loading data into media_spends")
    df = load_excel(filename,sheetname)
    df.to_sql(tablename, conn, index = False, if_exists = 'replace')

try:
    while True:
        log.info(str(datetime.datetime.today().strftime('%Y-%m-%d')) + ":Sleeping for 10 seconds")
        time.sleep(10)
        log.info("Checking for file _SUCCESS")
        if os.path.isfile(inputdir +"/_SUCCESS")== True:
            for f in os.listdir(inputdir):
                if f.endswith(".xlsx"):
                    log.info("Processing Xlsx files")
                    f = inputdir + "/" + f
                    processfile(f,sheetname,tablename)
                    log.info("Deleting _SUCCESS file")
                    os.system("rm " + inputdir + "/_SUCCESS")
                    log.info("Moving data file to backup dir")
                    os.system("mv " + f + " " + backupdir)
except Exception as e:
    log.info(str(e))

