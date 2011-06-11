#!/usr/bin/env python

import psycopg2, psycopg2.extensions
import sys
import getopt
import time

def usage():
    print """
Import Linz LandOnline titles, using a multipolygon as a collection of parcels. Intended for use in building titles data layer for Koordinates.

Usage:
    %s -y <year> --database-dsn <database dsn> [DEBUG]
    
    -y: The two digit ArcShape data year (e.g. 07)
    
    --database-dsn: Database DSN - eg. "host=db.example.com dbname=exampledb user=frog password=stomp"
    
    DEBUG: Only do parcels in/around Auckland

"""    % (sys.argv[0])

def main():
    # get the configuration options
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'y:d:', ['year', 'host=', 'database-dsn=', 'DEBUG'])
    except getopt.GetoptError:
        # print help information and exit:
        usage()
        sys.exit(2)
    dbdsn = None
    year = None
    for o, a in opts:
        if o in ('-y', '--year'):
            year = a
        if o in ('-d', '--database-dsn'):
            dbdsn = a

    debug = 'DEBUG' in args

    if (not (dbdsn and year)):
        usage()
        sys.exit(2)

    print "Connecting to database..."
    dbcon = psycopg2.connect(dbdsn)
    dbcon.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    dbcur = dbcon.cursor()
    
    try:
        dbcur.execute("select count(*) from nz_parcels")
        row = dbcur.fetchone()
        assert(row[0] > 0)
    except:
        raise Exception, "Make nz_parcels first!"
    
    try:
        dbcur.execute("drop table nz_titles;")
    except:
        pass #table may not exist yet
        
    print "Creating table..."
    dbcur.execute("""
    CREATE TABLE nz_titles
    (
      id serial NOT NULL PRIMARY KEY,
      title_no character varying NOT NULL,
      proprietors character varying,
      au_name character varying,
      ua_name character varying
    )
    WITHOUT OIDS""")
    
    print "Adding geometry column..."
    dbcur.execute("select addgeometrycolumn('public', 'nz_titles', 'geom', 2193, 'MULTIPOLYGON', 2)")
    
    print "Importing titles."
    
    sql = """
        insert into nz_titles (title_no, proprietors, au_name, ua_name, geom) (
            select nz_parcels.title_no,
                nz_parcels.proprietors,
                nz_parcels.au_name,
                nz_parcels.ua_name,
                multi(collect(nz_parcels.geom))
            from nz_parcels
            where title_no is not null

            %(debug)s
            group by title_no, proprietors, au_name, ua_name
        )""" % {'y':year, 'debug': debug and "and nz_parcels.geom && setsrid('BOX(1745000 5910000,1760000 5925000)'::box2d, 2193)" or ""}
    print sql
    dbcur.execute(sql)
        
    print "Creating spatial index..."
    dbcur.execute("create index idx_nz_titles_geom on nz_titles using gist(geom gist_geometry_ops)")

    print "Committing changes..."
    dbcon.commit()

    print "Finished!"

if __name__ == '__main__':
    main()
