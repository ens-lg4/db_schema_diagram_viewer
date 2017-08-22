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

    table_res   = mysql_engine.execute('SELECT table_name FROM INFORMATION_SCHEMA.TABLES'
                                   +' WHERE TABLE_TYPE="BASE TABLE" AND table_schema="' + database_name + '"')
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


def draw_table_node( graph, table_name, column_names, fillcolor='black' ):
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

    graph.edge( from_table + ':port_' + from_column + ':w', to_table + ':port_' + to_column + ':e' )


def draw_schema_diagram( url ):
    """Draw a schema diagram given the database url
    """

    meta        = {
        'hive_meta'                 :   'purple',
        'analysis_base'             :   '#C70C09',
        'dataflow_rule'             :   '#C70C09',
        'dataflow_target'           :   '#C70C09',
        'analysis_stats'            :   '#C70C09',
        'pipeline_wide_parameters'  :   '#C70C09',
        'analysis_ctrl_rule'        :   'pink',
        'resource_class'            :   '#FF7504',
        'resource_description'      :   '#FF7504',
        'job'                       :   '#1D73DA',
        'semaphore'                 :   '#1D73DA',
        'job_file'                  :   '#1D73DA',
        'accu'                      :   '#1D73DA',
        'analysis_data'             :   '#1D73DA',
        'worker'                    :   '#24DA06',
        'role'                      :   '#24DA06',
        'beekeeper'                 :   '#24DA06',
        'worker_resource_usage'     :   '#F4D20C',
        'log_message'               :   '#F4D20C',
        'analysis_stats_monitor'    :   '#F4D20C',
    }

    alch_url    = re.sub('^mysql://', 'mysql+pymysql://', url)
    dbname      = re.search('/(\w+)$', url).group(1)

    engine      = sqlalchemy.create_engine( alch_url )
    schema      = fetch_mysql_schema( engine, dbname )

    main_graph  = graphviz.Digraph( format='png' )
    main_graph.graph_attr['rankdir'] = 'RL'
    main_graph.graph_attr['concentrate'] = 'true'
    for table_name,attrib in schema.items() :
        draw_table_node( main_graph, table_name, attrib['columns'], fillcolor = meta.get(table_name) )

        for fkey in attrib['fkeys'] :
            table_column, fk_table_name, fk_table_column = fkey
            draw_fkey( main_graph, table_name, table_column, fk_table_name, fk_table_column )

    # print(main_graph.source)
    main_graph.render('table_diagram', view=True);


url = sys.argv[1] if sys.argv[1:] else os.environ['EHIVE_TEST_PIPELINE_URLS']

draw_schema_diagram( url )
