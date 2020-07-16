import psycopg2
from sqlalchemy import create_engine
import pandas as pd

def connect_db(connection_txt):

    connection_info = {}
    with open(connection_txt) as txt:
        for line in txt.readlines():
            if ';' not in line:
                continue
            param_name, param_value = line.split(';')
            connection_info[param_name.strip()] = param_value.strip()

    try:
        engine = create_engine(
            'postgresql://{username}:{password}@{ip_address}:{port}/{db_name}'.format(**connection_info))
    except:
        message = '\n\t' + '\n\t'.join(['%s: %s' % (k, v) for k, v in connection_info.iteritems()])
        raise ValueError('could not establish connection with parameters:%s' % message)

    return engine


def get_lookup_table(engine=None, table=None, index_col='code', value_col='name', conn=None):
    ''' Return a dictionary of code: name pairs from a given lookup table'''

    #with engine.connect() as conn, conn.begin():
    if not conn:
        conn = engine.connect()
        close_conn = True
    else:
        close_conn = False

    sql = "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    table_names = pd.read_sql(sql, conn).squeeze()

    if table in table_names.values:
        data = pd.read_sql("SELECT * FROM %s" % table, conn)
    else:
        table_options = '\n\t\t'.join(sorted(table_names.tolist()))
        raise ValueError('Table named "%s" not found. Options:\n\t\t%s"' % (table, table_options))

    # If the connection was passed from another function, don't close it. Otherwise this function created it, so close it
    if close_conn:
        conn.close()

    if index_col not in data.columns:
        raise ValueError('index_col "%s" not found in table columns: %s' % (index_col, ', '.join(data.columns)))
    if value_col not in data.columns:
        raise ValueError('value_col "%s" not found in table columns: %s' % (value_col, ', '.join(data.columns)))

    data.set_index(index_col, inplace=True)

    return data[value_col].to_dict()


def get_db_columns(table_name, engine=None, conn=None):

    if not conn:
        with engine.connect() as conn, conn.begin():
            db_columns = pd.read_sql(
                "SELECT column_name FROM information_schema.columns WHERE table_name = '%s';" % table_name,
                conn
            )
    else:
        db_columns = pd.read_sql(
            "SELECT column_name FROM information_schema.columns WHERE table_name = '%s';" % table_name,
            conn
        )

    return db_columns.squeeze()