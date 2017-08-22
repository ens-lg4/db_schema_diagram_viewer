#!/usr/bin/env python3

import sqlalchemy
import graphviz
import re
import sys
import os

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


def draw_table_node( graph, table_name, column_names, fillcolor='grey' ):
    """Create a graphviz node for a table
    """

    fillcolor = fillcolor or 'brown'

    contents = '<<table border="0"><th><td>' + table_name + '</td></th>'
    for column_name in column_names :
        contents = contents + '<tr><td bgcolor="white" port="port_' + column_name + '">' + column_name + '</td></tr>'
    contents = contents + '</table>>'

    graph.node( table_name, label=contents, shape='rectangle', style='rounded,filled', fillcolor=fillcolor )


def draw_fkey( graph, from_table, from_column, to_table, to_column ):
    """Create a graphviz edge for a foreign key
    """

    graph.edge( from_table + ':port_' + from_column + ':e', to_table + ':port_' + to_column + ':w' )


def draw_schema_diagram( url, meta_data ):
    """Draw a schema diagram given the database url
    """

    cluster_of  = {}
    for cluster_name,cluster_attribs in meta_data.items() :
        for table_name in cluster_attribs['tables'] :
            cluster_of[table_name] = cluster_name,cluster_attribs

    if re.match('^mysql://', url) :
        alch_url    = re.sub('^mysql://', 'mysql+pymysql://', url)
        dbname      = re.search('/(\w+)$', url).group(1)
        engine      = sqlalchemy.create_engine( alch_url )
        schema      = fetch_mysql_schema( engine, dbname )
    elif re.match('^pgsql://', url) :
        alch_url    = re.sub('^pgsql://', 'postgresql+pygresql://', url)
        dbname      = re.search('/(\w+)$', url).group(1)
        engine      = sqlalchemy.create_engine( alch_url )
        schema      = fetch_pgsql_schema( engine, dbname )
    else :
        exit(0)

    main_graph  = graphviz.Digraph( format='png' )
    main_graph.graph_attr['rankdir'] = 'LR'
    main_graph.graph_attr['concentrate'] = 'true'

    for table_name,attrib in schema.items() :
        cluster_pair    = cluster_of.get(table_name)
        if cluster_pair :
            cluster_name,cluster_attribs    = cluster_pair
            if not cluster_attribs.get('cluster_object') :
                special_graph   = graphviz.Digraph(name='cluster_'+cluster_name)
                special_graph.attr(style='filled', color=cluster_attribs['tone_colour'], fillcolor=cluster_attribs['tone_colour'])
                cluster_attribs['cluster_object'] = special_graph
            else :
                special_graph   = cluster_attribs['cluster_object']
            container_graph = special_graph
            table_colour    = cluster_attribs['table_colour']
        else :
            container_graph = main_graph
            table_colour    = 'grey'
        draw_table_node( container_graph, table_name, attrib['columns'], fillcolor = table_colour )

        for fkey in attrib['fkeys'] :
            table_column, fk_table_name, fk_table_column = fkey
            draw_fkey( main_graph, table_name, table_column, fk_table_name, fk_table_column )

    for cluster_attribs in meta_data.values() :
        special_graph   = cluster_attribs.get('cluster_object')
        if special_graph :
            main_graph.subgraph(special_graph)
    main_graph.render('table_diagram', view=True);


meta_data   = {
    'red' : {
        'tables'        : {
                            'analysis_base',
                            'dataflow_rule',
                            'dataflow_target',
                            'analysis_stats',
                            'pipeline_wide_parameters',
                          },
        'table_colour'  : '#C70C09',
        'tone_colour'   : '#FFDDDD',
    },
    'orange' : {
        'tables'        : {
                            'resource_class',
                            'resource_description',
                          },
        'table_colour'  : '#FF7504',
        'tone_colour'   : '#FFEEDD',
    },
    'blue' : {
        'tables'        : {
                            'job',
                            'semaphore',
                            'job_file',
                            'accu',
                            'analysis_data',
                          },
        'table_colour'  : '#1D73DA',
        'tone_colour'   : '#DDEEFF',
    },
    'green' : {
        'tables'        : {
                            'worker',
                            'role',
                            'beekeeper',
                          },
        'table_colour'  : '#24DA06',
        'tone_colour'   : '#DDFFDD',
    },
    'yellow' : {
        'tables'        : {
                            'worker_resource_usage',
                            'log_message',
                            'analysis_stats_monitor',
                          },
        'table_colour'  : '#F4D20C',
        'tone_colour'   : '#FFFFDD',
    },
}

url = sys.argv[1] if sys.argv[1:] else os.environ['EHIVE_TEST_PIPELINE_URLS']

draw_schema_diagram( url, meta_data )
