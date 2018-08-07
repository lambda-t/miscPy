import psycopg2
from psycopg2.pool import ThreadedConnectionPool
import psycopg2.extras

#jdbc:redshift://frillycluster.cydlhimdxe2x.us-west-1.redshift.amazonaws.com:5439/analytics

import os
import logging
import time
from parallel_connection.parallel_connection import ParallelConnection
import asyncio
import asyncpg


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
def data_pull():
    dbname = "ganalytic"
    host = "ganalytic.cvqb8lx7aald.us-west-2.redshift.amazonaws.com"
    port = 5439
    user = "root"
    password = "Haha123."

    """
    dbname = "analytics"
    host = "frillycluster.cydlhimdxe2x.us-west-1.redshift.amazonaws.com"
    port = "5439"
    user = "admin"
    password = "Frilly2017"
    """
    a = time.time()
    con=psycopg2.connect(dbname= dbname, host= host,
                         port= port, user= user, password= password)

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)



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
    #cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = 'media_spends_raw'")

    #cur.execute("SELECT * from explorer limit 10")
    cur.execute("SELECT *  FROM explorer WHERE date > '2017-12-15' and date < '2018-02-15'")
    #cur.execute("drop table ")
    e = time.time()
    print(e-s)
    b =time.time()
    print(b-a)

    #colnames = [desc[0] for desc in cur.description]
    #print(cur.fetchall())

    #print(cur.fetchall())
    return cur.fetchall()

    cur.close()
    con.close()


def data_pull2():
    dbname = "ganalytic"
    host = "ganalytic.cvqb8lx7aald.us-west-2.redshift.amazonaws.com"
    port = 5439
    user = "root"
    password = "Haha123."

    """
    dbname = "analytics"
    host = "frillycluster.cydlhimdxe2x.us-west-1.redshift.amazonaws.com"
    port = "5439"
    user = "admin"
    password = "Frilly2017"
    """
    a = time.time()
    con=psycopg2.connect(dbname= dbname, host= host,
                         port= port, user= user, password= password)

    #cur = con.cursor()



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
    #cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = 'media_spends_raw'")
    engine = 'postgresql'
    db_name = "ganalytic"
    host = "ganalytic.cvqb8lx7aald.us-west-2.redshift.amazonaws.com"
    port = 5439
    username = "root"
    password = "Haha123."
    dsns  = [engine + "://" + username + ":" + password + "@" + host + ":" + str(port) + "/" + db_name]
    n = 10 # maximum connections per pool
    pools = [ThreadedConnectionPool(1, n, dsn=d) for d in dsns]
    connections = [p.getconn() for p in pools]
    pdb = ParallelConnection(connections)
    c = pdb.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


    c.execute("SELECT * from explorer limit 10000")
    results = c.fetchall()
    c.close
    #cur.execute("drop table ")
    e = time.time()
    print(e-s)
    b =time.time()
    print(b-a)
    return results

    #colnames = [desc[0] for desc in cur.description]
    #print(cur.fetchall())

    #print(cur.fetchall())

def data_pull3():
    async def run():
        dbname = "ganalytic"
        host = "ganalytic.cvqb8lx7aald.us-west-2.redshift.amazonaws.com"
        port = 5439
        user = "root"
        password = "Haha123."

        """
        dbname = "analytics"
        host = "frillycluster.cydlhimdxe2x.us-west-1.redshift.amazonaws.com"
        port = "5439"
        user = "admin"
        password = "Frilly2017"
        """
        a = time.time()
        con= await asyncpg.connect(database= dbname, host= host,
                             port= port, user= user, password= password)




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
        #cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = 'media_spends_raw'")

        values = await con.fetch('''SELECT * from explorer limit 10000''')
        await con.close()
        #cur.execute("drop table ")
        e = time.time()
        print(e-s)
        b =time.time()
        print(b-a)

        #colnames = [desc[0] for desc in cur.description]
        #print(cur.fetchall())

        #print(values)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())




#data_pull()
