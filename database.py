from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# AstraDB Client and Secret required for the connection
ASTRA_CLIENT_ID = 'NhuSFqEEcpdsBWlPNSsymQjh'
ASTRA_SECRET = 'UTl0UHT0p+2plW-iT_u3XLxpc_r9z_jTF7TvpJFpNCiwlU5gbvrBNMGRUqLhv476wBYgbZ9NdskRHl9+7M+3uXRKbOpCZTjzYtF4cEcqmkn6kKCaxqy,DubbfsbLCqN.'

# Cloud Configuration
cloud_config = {
    'secure_connect_bundle': 'connect_bundle/secure-connect-e-commerce-db.zip'
}

# Authentication Provider
auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_SECRET)

# session object whIch is used foR interacting with Cassandra database
session = None


# Method for creating session object based on cloud configuration and Auth Provider
def create_session():
    global session
    try:
        if session is not None:
            print("Session initialised successfully.")
        else:
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
    except Exception as e:
        print("Error in initialising session : ", str(e))
        return None
    else:
        return session


# Method for setting keyspace in the session
# Input Parameters:
#    Session object
#    Keyspace_name
def set_session_keyspace(session, keyspace_name):
    try:
        session.execute(f'USE {keyspace_name}')
    except Exception as e:
        print("Error in setting keyspace in session", str(e))


# Method for executing query using the given session object
# Input Parameters:
#    Session object
#    query for execution        
def execute_query(session, query):
    try:
        return session.execute(query)
    except Exception as e:
        print("Error in executing query : ", str(e))


# Method for executing single row query using the given session object
# Input Parameters:
#    Session object
#    query for execution         
def execute_single_row_query(session, query):
    try:
        return session.execute(query).one()
    except Exception as e:
        print("Error in executing the query: ", str(e))


# Method for fetching table data using the given session object
# Input Parameters:
#    Session object
#    table name for which the data is to be fetched    
def show_table_data(session, table_name):
    rows=None
    try:
        rows=session.execute(f"select * from {table_name}")
    except Exception as e:
        print("Error in fetching data from table : ", str(e))
    else:
        for row in rows:
            print(row)
