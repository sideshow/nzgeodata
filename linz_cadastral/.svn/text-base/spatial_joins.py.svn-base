#!/usr/bin/env python

import psycopg2, psycopg2.extensions
import sys
import getopt
import time

def usage():
    print """
Precomputes spatial joins for CRS data. Takes a LONG time! You have been warned ...

Usage:
    %s -y <year> --database-dsn <database dsn>
    
    -y: The two digit ArcShape data year (e.g. 07)

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
        if o in ('-y', '--year'):
            year = a
        if o in ('-d', '--database-dsn'):
            dbdsn = a

    if (not (dbdsn and year)):
        usage()
        sys.exit(2)
    
    
    print "Connecting to database..."
    dbcon = psycopg2.connect(dbdsn)
    dbcon.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    dbcur = dbcon.cursor()
    
    dbcur.execute("select min(id), max(id) from crs_street_address;")
    min_id, max_id = dbcur.fetchone()
    step = 100
    parcel_update_sqls = ["update crs_street_address set parcel_id = (select id from crs_parcel where crs_parcel.toc_code = 'PRIM' and crs_parcel.shape && crs_street_address.shape and contains(crs_parcel.shape, crs_street_address.shape) limit 1) where crs_street_address.id >= %d and crs_street_address.id < %d;" % (i, i + step) for i in range(min_id, max_id + step + 1, step)]
    
    sqls = [
         "alter table crs_street_address add column meshblock_gid integer;",
         "alter table crs_street_address add column parcel_id integer;",
         "update crs_street_address set meshblock_gid = (select gid from mb%(year)s where the_geom && crs_street_address.shape and contains(the_geom, crs_street_address.shape));" % {'year': year},
    ]
    sqls.extend(parcel_update_sqls)
    sqls.extend([
         "create index idx_crs_street_address_meshblock_gid on crs_street_address ( meshblock_gid );",
         "create index idx_crs_street_address_parcel_id on crs_street_address (parcel_id);",
         "vacuum analyze crs_street_address;",
    ])
    
    i = 0
    for sql in sqls:
        i += 1
        try:
            dbcur.execute(sql)
            print "succeeded (rowcount=%d) (%d of %d)" % (dbcur.rowcount, i, len(sqls))
        except:
            #indexes may already exist
            print "FAILED:", sql, "(%d of %d)" % (i, len(sqls))

    print "Finished!"

if __name__ == '__main__':
    main()
