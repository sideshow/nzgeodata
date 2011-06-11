#!/usr/bin/env python

import psycopg2, psycopg2.extensions
import sys
import getopt
import time

def usage():
    print """
Import Linz LandOnline street addresses/titles. Intended for use in building geocoding layer for Koordinates.

Usage:
    %s -y <year> --database-dsn <database dsn>
    
    -y: The two digit ArcShape data year (e.g. 07)
    
    --database-dsn: Database DSN - eg. "host=localhost dbname=exampledb user=frog password=stomp"
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
    
        
    try:
        dbcur.execute("drop table nz_addresses;")
    except:
        pass #table may not exist yet
    
    print "Creating table..."
    sql = """
    CREATE TABLE nz_addresses
    (
      id serial NOT NULL,
      sad_id integer NOT NULL,
      house_num character varying,
      range_low integer,
      range_high integer,
      road_name character varying,
      title_no character varying,
      proprietors character varying,
      meshblock integer,
      au_name character varying,
      ua_name character varying,
      ta_name character varying,
      regc_name character varying,
      CONSTRAINT nz_addresses_pkey PRIMARY KEY (id)
    )
    WITHOUT OIDS"""
    print sql
    dbcur.execute(sql)
    
    print "Adding geometry column..."
    dbcur.execute("select addgeometrycolumn('public', 'nz_addresses', 'geom', 2193, 'POINT', 2)")
    
    print "Importing addresses."

    dbcur.execute("""
        insert into nz_addresses (sad_id, house_num, range_low, range_high, road_name, title_no, proprietors, geom, meshblock, au_name, ua_name, ta_name, regc_name) (
            select  crs_street_address.id,
                crs_street_address.house_number,
                crs_street_address.range_low,
                crs_street_address.range_high,
                asp_street.name,
                crs_title.title_no,

                array_to_string(array((
                    select distinct
                    coalesce(
                        case    when nmi.name_type = 'PERS' then trim(nmi.other_names || ' ' || nmi.surname)
                            else nmi.corporate_name
                            end
                        , '')
                    from crs_nominal_index nmi
                    where nmi.ttl_title_no = crs_title.title_no
                    )), '; '),
                crs_street_address.shape,
                mb%(y)s.mb%(y)s::integer,
                au%(y)s.au_name,
                ua%(y)s.ua_name,
                ta%(y)s.ta_name,
                regc%(y)s.regc_name
            from crs_street_address, mb%(y)s, au%(y)s, ua%(y)s, ta%(y)s, regc%(y)s, crs_road_name, crs_title, crs_legal_desc_prl, crs_legal_desc, asp_street
            where   crs_street_address.meshblock_gid = mb%(y)s.gid
            and mb%(y)s.au%(y)s = au%(y)s.au%(y)s
            and     mb%(y)s.ua%(y)s = ua%(y)s.ua%(y)s
            and mb%(y)s.ta%(y)s = ta%(y)s.ta%(y)s
            and mb%(y)s.regc%(y)s = regc%(y)s.regc%(y)s
            and crs_street_address.rna_id = crs_road_name.id
            and crs_legal_desc_prl.par_id = crs_street_address.parcel_id
            and crs_legal_desc.id = crs_legal_desc_prl.lgd_id
            and crs_title.title_no = crs_legal_desc.ttl_title_no
            and asp_street.sufi = crs_road_name.location
        )""" % {'y':year})
        
    print "Creating spatial index..."
    dbcur.execute("create index idx_nz_addresses_geom on nz_addresses using gist(geom gist_geometry_ops)")
    
    dbcon.commit()
    print "Finished!"

if __name__ == '__main__':
    main()
