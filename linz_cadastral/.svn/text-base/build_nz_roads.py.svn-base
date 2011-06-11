#!/usr/bin/env python

import psycopg2, psycopg2.extensions
import sys
import getopt
import time

def usage():
    print """
Import Linz LandOnline roads. Intended for use in building road centreline layer for Koordinates.

Usage:
    %s -y <year> --database-dsn <database dsn>
    
    --database-dsn: Database DSN - eg. "host=db.example.com dbname=exampledb user=frog password=stomp"

"""    % (sys.argv[0])

def main():
    # get the configuration options
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'y:d:', ['year', 'host=', 'database-dsn='])
    except getopt.GetoptError:
        # print help information and exit:
        usage()
        sys.exit(2)
    dbdsn = None
    year = None
    for o, a in opts:
        if o in ('-d', '--database-dsn'):
            dbdsn = a

    if (not dbdsn):
        usage()
        sys.exit(2)
    
    print "Connecting to database..."
    dbcon = psycopg2.connect(dbdsn)
    dbcon.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    dbcur = dbcon.cursor()
    
        
    try:
        dbcur.execute("drop table nz_roads;")
    except:
        pass #table may not exist yet
    
    print "Creating table..."
    dbcur.execute("""
    CREATE TABLE nz_roads
    (
      id integer NOT NULL PRIMARY KEY,
      name varchar,
      type varchar(4),
      sufi varchar,
      unofficial boolean DEFAULT false,
      othername1 varchar NULL,
      othername2 varchar NULL
    )
    WITHOUT OIDS""")
    
    print "Adding geometry column..."
    dbcur.execute("select addgeometrycolumn('public', 'nz_roads', 'geom', 2193, 'MULTILINESTRING', 2)")
    
    print "Importing roads."

    dbcur.execute("""
    INSERT INTO nz_roads (id, name, type, sufi, unofficial, geom, othername1, othername2) (
        SELECT
            c.id, coalesce(asp.name, n.name), n.type, n.location AS sufi,
            (n.unofficial_flag = 'Y')::boolean AS unofficial,
            multi(
                linemerge(
                    collect(
                        collect(c.shape, o2.shape),
                        o3.shape
                    )
                )
            ) as geom,
            o2.name AS othername1, o3.name AS othername2
        FROM
        crs_road_name n JOIN crs_road_name_asc a ON a.rna_id = n.id
        JOIN crs_road_ctr_line c ON c.id = a.rcl_id
    LEFT JOIN asp_street AS asp ON asp.sufi = n.location
        LEFT JOIN (
            SELECT coalesce(n2.name, asp2.name) as name, c2.shape, c2.id
            FROM crs_road_name n2, crs_road_ctr_line c2, crs_road_name_asc a2, asp_street asp2
            WHERE a2.rcl_id = c2.id AND a2.priority = 2 AND n2.id = a2.rna_id AND n2.location = asp2.sufi
        ) AS o2 ON o2.id = c.id
        LEFT JOIN (
            SELECT coalesce(n3.name, asp3.name) as name, c3.shape, c3.id
            FROM crs_road_name n3, crs_road_ctr_line c3, crs_road_name_asc a3, asp_street asp3
            WHERE a3.rcl_id = c3.id AND a3.priority = 3 AND n3.id = a3.rna_id AND n3.location = asp3.sufi
        ) AS o3 ON o3.id = c.id

        WHERE a.priority = 1 AND a.type <> 'RLWY'
    )
        """)
        
    print "Creating spatial index..."
    dbcur.execute("create index idx_nz_roads_geom on nz_roads using gist(geom gist_geometry_ops)")
    
    dbcon.commit()
    print "Finished!"

if __name__ == '__main__':
    main()
