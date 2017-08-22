# db_schema_diagram_viewer
Generate a database schema diagram given eHive-style database URL

Some GraphViz design ideas borrowed from (or shared with) @muffato's
    https://github.com/Ensembl/ensembl-hive/blob/master/scripts/dev/sql2rst.pl

But the input interface is different:
    sql2rst.pl reads schema information from a properly annotated SQL script.
    This project reads schema information from an existing SQL database.

