#!/usr/bin/env python

import sys
import getopt
import pprint
import psycopg2
import StringIO

def usage():
    print """
LINZ LandOnline data import script. Imports a single .crs file into a table
named the same in a PostGIS database.

Usage:
    %s --file <source file> --database-dsn <database dsn>
    
    file, f: Path to a LINZ CRS source file
    
    database-dsn, d: Database DSN - eg. "host=db.example.com dbname=exampledb user=frog password=stomp"

"""    % (sys.argv[0])

def main():
    print "Good Morning!"

    # get the configuration options
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'f:d:', ['file', 'host=', 'database-dsn='])
    except getopt.GetoptError:
        # print help information and exit:
        usage()
        sys.exit(2)

    dbdsn = None
    infilename = None
    for o, a in opts:
        if o in ('-f', '--file'):
            infilename = a
        if o in ('-d', '--database-dsn'):
            dbdsn = a

    if (not (dbdsn and infilename)):
        usage()
        sys.exit(2)
    
    print "Connecting to database..."
    dbcon = psycopg2.connect(dbdsn)

    dbcur = dbcon.cursor()
    #dbcur.execute("DELETE FROM %s" % hdr['TABLE'])
    
    
    # open the source file
    print "Opening source datafile: %s" % infilename
    crsfile = open(infilename, 'r')
    import_stuff(crsfile, dbcur)
    
    print "Committing DB Transaction..."
    dbcon.commit()
    
    print "All done :)"

def import_stuff(crsfile, dbcur):

    ENCODING = 'ISO8859-1'
    
    # read the file header
    print "Reading file header..."
    hdr = read_header(crsfile, ENCODING)
    pprint.pprint(hdr)
    
    # check versions
    if (hdr['HEDR'] != '1.0.0') or (hdr['SCHEMA'] != 'V1.0'):
        print "File Header/Table Schema version != 1.0\nFile Header:"

    print "Processing table: %s" % (hdr['TABLE'])

    cols = hdr['COLUMN']
    colstr = ""
    valstr = ""
    for col in cols:
        colstr += col[0] + ','
        if col[1] in ('geometry', 'st_geometry'):
            # geometries are NZGD2000/LatLong but offset by -160 degrees
            valstr += "Transform(Translate(GeomFromText(%s, 4167), 160, 0, 0),2193),"
        elif col[1] == 'crs_fraction':
            valstr += "(1.0/split_part(%s, '/', 2)::integer)::real,"
        else:
            valstr += '%s,'
    colstr = colstr[:-1]
    valstr = valstr[:-1]
    sql = "INSERT INTO %s (%s) VALUES (%s);" % (hdr['TABLE'], colstr, valstr)
    print "Insert SQL: [%s]" % (sql)
    
    
    data_start = False
    print "Inserting data..."
    rowcount = 0
    params = []
    while True:
        try:
            line = crsfile.readline().decode(ENCODING)
            while line[-2:] == u'\\\n':
                line += u"\n" + crsfile.readline().decode(ENCODING)
                rowcount += 1
            
            if not line:
                break
            line = line.strip()

            fields = line.split(u'|')
            for f in range(len(fields) - 1):
                if len(fields) <= f: break
                if len(fields[f]) < 1: continue
                nextField = fields[f]
                while len(nextField) >= 1 and nextField[-1] == u'\\':
                    if len(fields[f]) >= 2 and fields[f][-2] == u'\\':
                        break #poor man's escaping!
                    fields[f] += fields[f+1]
                    nextField = fields.pop(f+1)
            
            cfields = []
            fields.pop() # bonus '|' at end of line
            csql = sql
            ignoreRow = False
            for ii in range(len(cols)):
                if cols[ii][0] == u'status' and fields[ii] in (u'HIST', u'HSTO'):
                    #historic data, ignore
                    ignoreRow = True
                if cols[ii][1] in (u'geometry', u'st_geometry'):
                    if len(fields[ii][2:]) > 0:
                        cfields.append(fields[ii][2:])
                    else:
                        cfields.append(None)
                else:
                    if fields[ii] == '':
                        cfields.append(None)
                    else:
                        cfields.append(fields[ii])
            sql = sql.replace('%%', '%')

            if not ignoreRow:
                params.append(tuple(cfields))
            rowcount += 1
            if (rowcount % 10000) == 0:
                dbcur.executemany(sql, params)
                print "%d..." % (rowcount)
                params = []
        except Exception, e:
            print "Error at rowcount=%d" % rowcount
            print "line='%s'" % line
            print "fields=", fields
            print "cols=", cols
            raise
    if params != []:
        dbcur.executemany(sql, params)
    
    
def read_header(crsfile, encoding):
    hdrinfo = { 'COLUMN':[] }
    while True:
        line = crsfile.readline().decode(encoding)
        if not line:
            break
        line = line.strip()
        if line == '{CRS-DATA}':
            break

        #print "\thdr: [%s]" % lt
        hl = line.split(None, 1)
        
        if hl[0] == 'COLUMN':
            hdrinfo['COLUMN'].append(hl[1].split(None,2))
        elif len(hl) > 1:
            hdrinfo[hl[0]] = hl[1]

    return hdrinfo

if __name__ == '__main__':
    main()
