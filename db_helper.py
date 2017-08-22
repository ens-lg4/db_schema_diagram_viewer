
import sqlalchemy
import re


def fetch_mysql_schema( mysql_engine, database_name ):
    """Fetch table/column and foreign key definition from a MySQL database
    """

    mysql_schema = {}

    table_res   = mysql_engine.execute('SELECT table_name FROM information_schema.tables'
                                   +' WHERE table_type="BASE TABLE" AND table_schema="' + database_name + '"')
    for table_row in table_res :
        table_name  = table_row[0]

        column_res  = mysql_engine.execute('DESCRIBE ' + table_name)
        columns     = [column_row['Field'] for column_row in column_res]
        fk_res      = mysql_engine.execute('SELECT * FROM information_schema.KEY_COLUMN_USAGE'
                                    + ' WHERE TABLE_SCHEMA="' + database_name + '" AND TABLE_NAME="' + table_name + '"'
                                    + ' AND REFERENCED_TABLE_NAME IS NOT NULL')
        fkeys       = [ (fk_row['COLUMN_NAME'],fk_row['REFERENCED_TABLE_NAME'],fk_row['REFERENCED_COLUMN_NAME'])
                        for fk_row in fk_res ]
        mysql_schema[table_name] = { 'columns' : columns, 'fkeys' : fkeys }

    return mysql_schema


def fetch_pgsql_schema( pgsql_engine, database_name ):
    """Fetch table/column and foreign key definition from a PostgreSQL database
    """

    pgsql_schema = {}

    table_res   = pgsql_engine.execute("""  SELECT table_name
                                            FROM information_schema.tables
                                            WHERE table_type='BASE TABLE'
                                            AND table_schema='public'
                                            AND table_catalog='""" + database_name + "'")

    for table_row in table_res :
        table_name  = table_row[0]

        column_res   = pgsql_engine.execute(""" SELECT column_name
                                                FROM information_schema.columns
                                                WHERE table_schema='public'
                                                AND table_catalog='""" + database_name + "' AND table_name='" + table_name + "'")

        columns     = [column_row[0] for column_row in column_res]

        fkeys       = {}

        fk_res      = pgsql_engine.execute("""  SELECT kcu.column_name,
                                                ccu.table_name AS foreign_table_name,
                                                ccu.column_name AS foreign_column_name
                                                FROM
                                                information_schema.table_constraints AS tc
                                                JOIN information_schema.key_column_usage AS kcu
                                                ON tc.constraint_name = kcu.constraint_name
                                                JOIN information_schema.constraint_column_usage AS ccu
                                                ON ccu.constraint_name = tc.constraint_name
                                                WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='""" + table_name + "'");

        fkeys       = [ (fk_row['column_name'],fk_row['foreign_table_name'],fk_row['foreign_column_name'])
                        for fk_row in fk_res ]
        pgsql_schema[table_name] = { 'columns' : columns, 'fkeys' : fkeys }

    return pgsql_schema


def fetch_sql_schema( url ):
    """Fetch table/column and foreign key definition from a MySQL/PostgreSQL database given an eHive-style URL
    """

    dbname      = re.search('/(\w+)$', url).group(1)

    if re.match('^mysql://', url) :
        alch_url    = re.sub('^mysql://', 'mysql+pymysql://', url)
        engine      = sqlalchemy.create_engine( alch_url )
        schema      = fetch_mysql_schema( engine, dbname )
    elif re.match('^pgsql://', url) :
        alch_url    = re.sub('^pgsql://', 'postgresql+pygresql://', url)
        engine      = sqlalchemy.create_engine( alch_url )
        schema      = fetch_pgsql_schema( engine, dbname )
    else :
        exit(0)

    return schema

