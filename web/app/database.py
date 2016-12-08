from app import app
from impala.dbapi import connect
from sshtunnel import SSHTunnelForwarder

def query_db(query):
    with SSHTunnelForwarder(
        app.config['SSH_HOST'],
        ssh_username=app.config['USERNAME'],
        ssh_password=app.config['PASSWORD'],
        remote_bind_address=(app.config['IMPALA_HOST'], app.config['IMPALA_PORT'])
    ) as tunnel:
        conn = connect(
            host=tunnel.local_bind_host,
            port=tunnel.local_bind_port,
            database='vision_zero'
        )

        cursor = conn.cursor()
        cursor.execute(query)

        field_names = [i[0] for i in cursor.description]
        results = cursor.fetchall()

        cursor.close()
        conn.close()

    return [tuple(field_names)] + results
