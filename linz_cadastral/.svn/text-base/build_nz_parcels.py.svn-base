#!/usr/bin/env python

import csv
import psycopg2, psycopg2.extensions
import sys
import getopt
import time

def usage():
    print """
Import Linz LandOnline parcels. Intended for use in building parcels data layer for Koordinates.

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

    if (not (dbdsn and year)):
        usage()
        sys.exit(2)

    debug = 'DEBUG' in args
    
    print "Connecting to database..."
    dbcon = psycopg2.connect(dbdsn)
    original_isolation_level = dbcon.isolation_level
    dbcon.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    dbcur = dbcon.cursor()
    update_cursor = dbcon.cursor()
    
        
    try:
        dbcur.execute("drop table nz_parcels;")
    except:
        pass #table may not exist yet
    
    print "Creating table..."
    dbcur.execute("""
    CREATE TABLE nz_parcels
    (
      id serial NOT NULL PRIMARY KEY,
      par_id integer NOT NULL,
      street_add character varying,
      title_no character varying,
      proprietors character varying,
      appellation character varying,
      au_name character varying,
      ua_name character varying
    )
    WITHOUT OIDS""")
    
    
    sub_types = {}
    r = csv.reader(open("appellation_sub_types.csv", "rb"))
    for key, value in r:
        sub_types[key] = value
    
    parcel_types = {}
    r = csv.reader(open("appellation_parcel_types.csv", "rb"))
    for key, value in r:
        parcel_types[key] = value
    
    print "Removing null geometries from input data"
    dbcur.execute("delete from crs_parcel where shape is null")
    dbcon.commit()
   
    print "Adding geometry column..."
    dbcur.execute("select addgeometrycolumn('public', 'nz_parcels', 'geom', 2193, 'POLYGON', 2)")
    dbcon.commit()
    
    
    print "Retrieving parcel data."

    dbcon.set_isolation_level(original_isolation_level)

    cursor1 = dbcon.cursor("cursor1")
    cursor1.execute("""
        select parcels.parcel_id as par_id,
            parcels.street_add as street_add,
            crs_title.title_no as title_no,
            array_to_string(array((
                select distinct
                coalesce(
                    case    when nmi.name_type = 'PERS' then trim(nmi.other_names || ' ' || nmi.surname)
                        else nmi.corporate_name
                        end
                    , '')
                from crs_nominal_index nmi
                where nmi.ttl_title_no = crs_title.title_no
                )), '; ') as proprietors,
            au%(y)s.au_name as au_name,
            ua%(y)s.ua_name as ua_name,
            parcels.shape as geom
        from
            (
                select crs_parcel.id as parcel_id, trim(array_to_string(array(
                (
                    SELECT sa.house_number
                    FROM crs_street_address as sa
                    WHERE sa.parcel_id = crs_parcel.id
                    ORDER BY sa.house_number ASC
                )), ',') || ' ' || coalesce(asp_street.name, '')) as street_add, crs_street_address.meshblock_gid as meshblock_gid, crs_parcel.shape as shape
                FROM crs_parcel LEFT JOIN crs_street_address ON crs_parcel.id = crs_street_address.parcel_id
                    LEFT JOIN crs_road_name ON crs_street_address.rna_id = crs_road_name.id
                    LEFT JOIN asp_street ON asp_street.sufi = crs_road_name.location
                GROUP BY crs_parcel.id, crs_parcel.shape, crs_street_address.rna_id, asp_street.name, meshblock_gid
            ) as parcels
            LEFT JOIN crs_legal_desc_prl ON crs_legal_desc_prl.par_id = parcels.parcel_id
            LEFT JOIN crs_legal_desc ON crs_legal_desc.id = crs_legal_desc_prl.lgd_id
            LEFT JOIN crs_title ON crs_title.title_no = crs_legal_desc.ttl_title_no
            LEFT JOIN mb%(y)s ON parcels.meshblock_gid = mb%(y)s.gid
            LEFT JOIN au%(y)s ON mb%(y)s.au%(y)s = au%(y)s.au%(y)s
            LEFT JOIN ua%(y)s ON mb%(y)s.ua%(y)s = ua%(y)s.ua%(y)s

        %(debug)s

        ORDER BY par_id ASC, street_add ASC, crs_legal_desc.type DESC
    """ % {'y':year, 'debug': debug and "and parcels.shape && setsrid('BOX(1745000 5910000,1760000 5925000)'::box2d, 2193)" or ""})
    
    c = 'par_id street_add title_no proprietors au_name ua_name geom'.split(' ')
    c = dict(zip(c, range(len(c))))
    
    par_id = -1
    street_add = None
    inserts = []
    updates = []

    step = 20000
    i = 0
    rows = cursor1.fetchmany(size=step)
    while rows:
        
        print "Collating/inserting parcel data: %d rows." % i
        for row in rows:
            if row[c['par_id']] != par_id:
                #new parcel, insert
                inserts.append(row)
                par_id = row[c['par_id']]
            else:
                #same as last parcel, update street address if different, otherwise ignore
                if row[c['street_add']] != street_add:
                    updates.append((row[c['street_add']], row[c['par_id']]))
            street_add = row[c['street_add']]
        update_cursor.executemany("insert into nz_parcels (par_id, street_add, title_no, proprietors, au_name, ua_name, geom) VALUES (%s, %s, %s, %s, %s, %s, %s)", inserts)
        update_cursor.executemany("update nz_parcels set street_add = street_add || ';' || %s where id = %s", updates)
        updates = []
        inserts = []
        rows = cursor1.fetchmany(size=step)
        i += step

    dbcon.commit()
   
    print "Fetching appellation data."
    cursor2 = dbcon.cursor("cursor2")
    cursor2.execute("""
        SELECT a.par_id,
            trim(a.appellation_value) as appellation_value,
            trim(a.sub_type) as sub_type,
            trim(a.part_indicator) as part_indicator,
            trim(a.parcel_type) as parcel_type,
            trim(a.parcel_value) as parcel_value,
            trim(a.block_number) as block_number,
            trim(a.sub_type_position) as sub_type_position,
            trim(a.maori_name) as maori_name,
            trim(a.type) as type
        FROM crs_appellation AS a
        %(debug)s
    """ % {'debug': debug and ", crs_parcel WHERE a.par_id = crs_parcel.id and crs_parcel.shape && setsrid('BOX(1745000 5910000,1760000 5925000)'::box2d, 2193)" or ""})
    
    c = 'par_id appellation_value sub_type part_indicator parcel_type parcel_value block_number sub_type_position maori_name type'.split(' ')
    c = dict(zip(c, range(len(c))))
    
    appellations = []
    
    step = 20000
    i = 0
    rows = cursor2.fetchmany(size=step)
    while rows:

        print "Collating/inserting appellation data: %d rows." % i
    
        for row in rows:
            if row[c['part_indicator']] == 'PART':
                a_part = 'Pt'
            else:
                a_part = ''


            if row[c['parcel_type']] == '' or row[c['parcel_type']] is None or row[c['parcel_type']] not in parcel_types:
                a_parcel_type = ''
            else:
                a_parcel_type = parcel_types[row[c['parcel_type']]]


            if row[c['type']] == 'MAOR':
                a_name = row[c['maori_name']] or ''
            else:
                a_name = row[c['appellation_value']] or ''


            if row[c['block_number']]:
                a_block = "Blk %s" % row[c['block_number']]
            else:
                a_block = ''


            if row[c['sub_type']] and row[c['sub_type']] in sub_types:
                a_sub_type = sub_types[row[c['sub_type']]]
            else:
                a_sub_type = ''
            
            a_parcel_value = row[c['parcel_value']] or ''

            if a_parcel_type == '':
                appellation = [a_part, a_parcel_type, a_block, a_name, a_parcel_value, a_sub_type]
            elif row[c['sub_type_position']] == 'SUFX':
                appellation = [a_part, a_parcel_type, a_parcel_value, a_block, a_name, a_sub_type]
            else:
                appellation = [a_part, a_parcel_type, a_parcel_value, a_sub_type, a_block, a_name]
            appellation = " ".join([a for a in appellation if a != ''])
            appellations.append((appellation, row[c['par_id']]))
        
        update_cursor.executemany("UPDATE nz_parcels SET appellation = %s WHERE id = %s", appellations)
        appellations = []
        rows = cursor2.fetchmany(size=step)
        i += step
    dbcon.commit()
            
        
    print "Creating spatial index..."
    dbcur.execute("create index idx_nz_parcels_geom on nz_parcels using gist(geom gist_geometry_ops)")
    
    print "Finished!"

if __name__ == '__main__':
    main()
