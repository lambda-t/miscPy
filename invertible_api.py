# things.py

# Let's get this party started!
import falcon
import ujson
import pull_redshift_data
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

import simple_settings as settings


class Explorer(object):
    def on_get(self, req, resp,start_date,end_date):
        raw_json = req.stream.read()
        #req_json = ujson.loads(raw_json.decode('utf-8'))
        resp.status = falcon.HTTP_202
        #print(req_json)
        #q = "SELECT *  FROM explorer WHERE date > " + "'" + req_json['start_date'] + "'" + " and date < " + \
            #"'" +  req_json['end_date'] + "'"
        q = "SELECT *  FROM explorer WHERE date > " + "'" + start_date + "'" + " and date < " + \
            "'" +  end_date + "'"
        print(q)
        result= pull_redshift_data.data_pull(q)
        #result = Session.query(NonOrmTable).one()
        #print(result)
        resp.body = json.dumps(result)

    # falcon.API instances are callable WSGI apps
class Performance(object):
    def on_get(self, req, resp,start_date,end_date):
        raw_json = req.stream.read()
        #req_json = ujson.loads(raw_json.decode('utf-8'))
        resp.status = falcon.HTTP_202
        #print(req_json)
        #q = "SELECT *  FROM explorer WHERE date > " + "'" + req_json['start_date'] + "'" + " and date < " + \
        #"'" +  req_json['end_date'] + "'"
        q = "SELECT *  FROM dashboard_overview WHERE date > " + "'" + start_date + "'" + " and date < " + \
            "'" +  end_date + "'"
        print(q)
        result= pull_redshift_data.data_pull(q)
        #result = Session.query(NonOrmTable).one()
        #print(result)
        resp.body = json.dumps(result)

app = falcon.API()

# Resources are represented by long-lived class instances
explorer_data = Explorer()
performance_data = Performance()

# things will handle all requests to the '/things' URL path
app.add_route('/g_analytics/explorer/{start_date}/{end_date}', explorer_data)
app.add_route('/g_analytics/performance/{start_date}/{end_date}', performance_data)



#API_URL=http://52.35.90.133
"""
engine = 'postgresql'
db_name = "ganalytic"
host = "ganalytic.cvqb8lx7aald.us-west-2.redshift.amazonaws.com"
port = 5439
username = "root"
password = "Haha123."

engine = create_engine(engine + "://" + username + ":" + password + "@" + host + ":" + str(port) + "/" + db_name )


session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

class SQLAlchemySessionManager:



def __init__(self, Session):
    self.Session = Session

def process_resource(self, req, resp, resource, params):
    resource.session = self.Session()

def process_response(self, req, resp, resource, req_succeeded):
    if hasattr(resource, 'session'):
        Session.remove()

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table
Base = declarative_base(engine)

class NonOrmTable(Base):

#eg. fields: id, title

__tablename__ = 'media_spends_raw'
__table_args__ = {'autoload': True}
date = Column(Integer, primary_key=True)

table = Table('media_spends_raw', Base.metadata)
"""