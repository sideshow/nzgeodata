#!/usr/bin/env python

import sys
import getopt
import pprint
import psycopg2
import psycopg2.extensions
import os
import landonline_import

def usage():
    print """
Import Linz LandOnline data. Intended for use in building data layers for Koordinates.

Usage:
    %s -c <crs root path> --database-dsn <database dsn>
    
    -c: The LINZ LandOnline directory containing compressed CRS data
    
    --database-dsn: Database DSN - eg. "host=db.example.com dbname=exampledb user=frog password=stomp"

"""    % (sys.argv[0])

def main():
    # get the configuration options
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'c:d:', ['crsroot', 'host=', 'database-dsn='])
    except getopt.GetoptError:
        # print help information and exit:
        usage()
        sys.exit(2)
    dbdsn = None
    dirname = None
    for o, a in opts:
        if o in ('-c', '--crsroot'):
            dirname = a
        if o in ('-d', '--database-dsn'):
            dbdsn = a

    if (not (dbdsn and dirname)):
        usage()
        sys.exit(2)
    
    print "Connecting to database..."
    dbcon = psycopg2.connect(dbdsn)
    dbcon.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    dbcur = dbcon.cursor()
    
    #drop tables
    to_drop = ['crs_road_ctr_line', 'crs_road_name', 'crs_road_name_asc', 'crs_street_address', 'crs_parcel', 'crs_appellation', 'crs_statute_action', 'crs_stat_act_parcl', 'crs_title', 'crs_legal_desc', 'crs_legal_desc_prl', 'crs_nominal_index']
    for table in to_drop:
        try:
            dbcur.execute("drop table %s;" % table)
        except:
            pass

    #recreate tables
    dbcur.execute("""
        CREATE TABLE crs_road_name_asc ( rna_id integer NOT NULL, rcl_id integer NOT NULL, alt_id integer NULL, priority integer NOT NULL, audit_id bigint NOT NULL, PRIMARY KEY (rna_id, rcl_id));
        CREATE TABLE crs_road_ctr_line ( id integer NOT NULL PRIMARY KEY, alt_id integer NULL, status varchar NOT NULL, non_cadastral_rd char NOT NULL, se_row_id integer NULL, audit_id bigint NOT NULL);
        SELECT AddGeometryColumn('crs_road_ctr_line', 'shape', 2193, 'LINESTRING', 2);
        create table crs_road_name (id integer not null primary key, alt_id integer null, type varchar not null, name varchar not null, location varchar not null, status varchar not null, unofficial_flag char not null, audit_id serial not null);
        CREATE TABLE crs_street_address ( house_number varchar NOT NULL, range_low integer NOT NULL, range_high integer NULL, status varchar NOT NULL, unofficial_flag char NOT NULL, rcl_id integer NOT NULL, rna_id integer NOT NULL, alt_id integer NULL, se_row_id integer NULL, id integer NOT NULL, audit_id serial not null, PRIMARY KEY (id)) ;
        SELECT AddGeometryColumn('crs_street_address', 'shape', 2193, 'POINT', 2);
        create table crs_parcel (id integer PRIMARY KEY,ldt_loc_id integer NOT NULL,img_id integer NULL,fen_id integer NULL,toc_code varchar(4) NOT NULL,alt_id integer NULL,area decimal NULL,nonsurvey_def varchar(255) NULL,appellation_date varchar(255) NULL,parcel_intent varchar NOT NULL,status varchar(4) NOT NULL,total_area decimal NULL,calculated_area decimal NULL,se_row_id integer NULL,audit_id integer NOT NULL);
        SELECT AddGeometryColumn('crs_parcel', 'shape', 2193, 'GEOMETRY', 2);
        create table crs_appellation (id integer PRIMARY KEY, par_id integer, type character varying, title character, survey character, status character varying, part_indicator character varying, maori_name character varying, sub_type character varying, appellation_value character varying, parcel_type character varying, parcel_value character varying, second_parcel_type character varying, second_prcl_value character varying, block_number character varying, sub_type_position character varying, act_id_crt integer, act_tin_id_crt integer, act_id_ext integer, act_tin_id_ext integer, audit_id integer, other_appellation character varying);
        create table crs_stat_act_parcl (sta_id integer, par_id integer, status character varying, action character varying, purpose character varying, name character varying, comments character varying, audit_id integer, PRIMARY KEY (sta_id, par_id));
        create table crs_statute_action (type character varying, status character varying, ste_id integer, sur_wrk_id_vesting integer, gazette_year integer, gazette_page integer, gazette_type character varying, other_legality character varying, recorded_date character varying, id integer PRIMARY KEY, audit_id integer);
        create table crs_title (title_no character varying NOT NULL PRIMARY KEY, ldt_loc_id integer NOT NULL, status character varying  NOT NULL, issue_date date NOT NULL, register_type character varying NOT NULL, type character varying NOT NULL, audit_id integer NOT NULL);
        create table crs_legal_desc (id integer NOT NULL PRIMARY KEY, type character varying NOT NULL, status character varying NOT NULL, ttl_title_no character varying NULL, audit_id integer NOT NULL, total_area decimal NULL, legal_desc_text character varying NULL );
        create table crs_legal_desc_prl (id serial NOT NULL PRIMARY KEY, lgd_id integer NOT NULL, par_id integer NOT NULL, sequence smallint NOT NULL, part_affected character varying NOT NULL, share real NOT NULL, audit_id integer NOT NULL, sur_wrk_id_crt integer NULL );
        create table crs_nominal_index (ttl_title_no character varying NOT NULL, prp_id integer NULL, id integer NOT NULL PRIMARY KEY, status character varying NOT NULL, name_type character varying NOT NULL, surname character varying NULL, other_names character varying NULL, corporate_name character varying NULL, audit_id integer NULL);
    """)

    #get source file names
    files = [os.path.join(dirname, f) for f in [
        'roadadd/rns.crs.Z',
        'roadadd/rcl1.crs.Z',
        'roadadd/rna.crs.Z',
        'parcel/sap.crs.Z',
        'parcel/sta.crs.Z',
        'parcel/app1.crs.Z',
        'title/ttl.crs.Z',
        'title/nmi.crs.Z',
        'parcel/lgd1.crs.Z',
        'parcel/lgp.crs.Z',
        'roadadd/sad1.crs.Z',
        'parcel/par1.crs.Z',
        'parcel/par9.crs.Z'
    ]]
    
    dbcon.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
    for f in files:
        print "Decompressing %s" % (f,)
        fileObj = os.popen('uncompress -c '+f, 'r')
        print "Importing from decompressed %s" % (f,)
        landonline_import.import_stuff(fileObj, dbcur)
        fileObj.close()
        dbcon.commit()
    
    
    #build indexes
    dbcon.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    print "Creating indexes on CRS tables..."
    
    indexes = [
        "update crs_parcel set shape = buffer(geometryn(multi(crs_parcel.shape), 1), 0) where shape is not null;"
        "create index idx_crs_parcel_toc_code on crs_parcel (toc_code);",
        "CREATE INDEX idx_crs_stat_act_parcl_sta_id on crs_stat_act_parcl (sta_id);",
        "CREATE INDEX idx_crs_stat_act_parcl_par_id on crs_stat_act_parcl (par_id);",
        "CREATE INDEX idx_crs_appellation_par_id on crs_appellation (par_id);",
        "CREATE INDEX idx_crs_parcel_shape on crs_parcel USING GIST (shape GIST_GEOMETRY_OPS);",
        "CREATE INDEX idx_crs_legal_desc_prl_lgd_id on crs_legal_desc_prl (lgd_id);",
        "CREATE INDEX idx_crs_legal_desc_prl_par_id on crs_legal_desc_prl (par_id);",
        "CREATE INDEX idx_crs_legal_desc_ttl_title_no on crs_legal_desc (ttl_title_no);",
        "CREATE INDEX idx_crs_nominal_index_ttl_title_no on crs_nominal_index (ttl_title_no);",
        "CREATE INDEX crs_street_address_idx_rna_id ON crs_street_address (rna_id);",
        "CLUSTER crs_street_address_pkey ON crs_street_address",
        "create index crs_road_name_asc_rna_id on crs_road_name_asc (rna_id);",
        "create index crs_road_name_asc_rcl_id on crs_road_name_asc (rcl_id);",
        "CREATE INDEX idx_crs_road_name_location on crs_road_name (location);",
        "CREATE INDEX crs_road_name_idx_status ON crs_road_name (status);",
        "CREATE INDEX crs_road_name_idx_name ON crs_road_name (name);",
        "create index idx_crs_road_ctr_line_shape on crs_road_ctr_line using gist(shape gist_geometry_ops);",
        "CREATE INDEX crs_street_address_idx_house_number ON crs_street_address (house_number);",
        "CREATE INDEX crs_street_address_idx_range_low ON crs_street_address (range_low);",
        "CREATE INDEX crs_street_address_idx_range_high ON crs_street_address (range_high);",
        "CREATE INDEX crs_street_address_idx_status ON crs_street_address (status);",
        "create index idx_crs_street_address_shape on crs_street_address using gist(shape gist_geometry_ops);",
        "CREATE INDEX crs_parcel_idx_ldt ON crs_parcel (ldt_loc_id);",
        "create index idx_crs_street_address_shape on crs_street_address using gist(shape gist_geometry_ops);",
    ]
    for sql in indexes:
        print sql
        try:
            dbcur.execute(sql)
            print "...succeeded"
        except:
            #indexes may already exist
            print "...failed"

    print "Finished! :)"

if __name__ == '__main__':
    main()
