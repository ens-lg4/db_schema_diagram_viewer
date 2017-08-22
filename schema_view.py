#!/usr/bin/env python3

import db_helper
import graphviz
import sys
import os
import json


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
        cluster_attribs['tables'] = set( cluster_attribs['tables'] )
        for table_name in cluster_attribs['tables'] :
            cluster_of[table_name] = cluster_name,cluster_attribs

    schema  = db_helper.fetch_sql_schema( url )

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


url         = sys.argv[1] if sys.argv[1:] else os.environ['EHIVE_TEST_PIPELINE_URLS']
json_fname  = sys.argv[2] if sys.argv[2:] else 'ehive_clusters.json'

meta_data   = json.load( open(json_fname, 'r') )

draw_schema_diagram( url, meta_data )
