import psycopg2

#jdbc:redshift://frillycluster.cydlhimdxe2x.us-west-1.redshift.amazonaws.com:5439/analytics

import os
import logging
import time
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

dbname = "ganalytic"
host = "ganalytic.cvqb8lx7aald.us-west-2.redshift.amazonaws.com"
port = 5439
user = "root"
password = "ShiftRed123."

"""
dbname = "analytics"
host = "frillycluster.cydlhimdxe2x.us-west-1.redshift.amazonaws.com"
port = "5439"
user = "admin"
password = "ShiftRed123."
"""
a = time.time()
con=psycopg2.connect(dbname= dbname, host= host,
                     port= port, user= user, password= password)

con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cur = con.cursor()



#cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")

#cur.execute("""SELECT table_name FROM information_schema.tables
#WHERE table_schema = 'public'""")


#cur.execute("SELECT * FROM pg_stat_user_tables;")


#cur.execute("CREATE TABLE ga_test (sessions INTEGER  NOT NULL,visits INTEGER NOT NULL, transactions INTEGER NOT NULL,"
#"bounces INTEGER NOT NULL, sessionDuration FLOAT NOT NULL,  deviceCategory VARCHAR(100) NOT NULL,medium VARCHAR(100) NOT NULL,  "
#"landingPagePath VARCHAR(300) NOT NULL,date VARCHAR(100) NOT NULL)")


#cur.execute("select * from pg_table_def where tablename = 'media_spends' and schemaname = 'public'")


#cur.execute("select * from ga_test")

#cur.execute("SELECT * FROM pg_stat_user_tables;")

#cur.execute("drop table if exists ga_test")
#cur.execute("drop table if exists ga_test")

s= time.time()
cur.execute("select count(*) from media_spends_raw")
#cur.execute("show databases")
e = time.time()
print(e-s)
b =time.time()
print(b-a)

#colnames = [desc[0] for desc in cur.description]
print(cur.fetchall())

#print(cur.fetchall())

cur.close()
con.close()