#!/usr/bin/env python

import os
import sys

def main():
    if not len(sys.argv) == 3:
        print """
Import the Statistics NZ Digital Boundaries. Just a shortcut for running shp2pgsql and
creating a few indexes.

Usage: %s arcshape_root year dbname
    e.g. %s data/Statistics\ NZ/Lv1_2007_Digital\ Boundaries/ARCSHAPE/ 07
        """ % (sys.argv[0],sys.argv[0])
        sys.exit(-1)
    root, year = sys.argv[1:]
    tables = ['%s%s' % (a, year) for a in ['au', 'mb', 'regc', 'ta', 'ua']]
    
    for name in tables:
        shapefile = os.path.join(os.path.join(root, name), name)
        print "Importing into table %s" % (name,)
        os.system("""/usr/lib/postgresql/8.2/bin/shp2pgsql -s 27200 "%s" %s | psql -d %s""" % (shapefile, name, dbname))
        os.system("""psql -d %s -c "alter table %s drop constraint enforce_srid_the_geom;" """ % (dbname,name,))
        os.system("""psql -d %s -c "update %s set the_geom = transform(the_geom, 2193);" """ % (dbname,name,))
        os.system("""psql -d %s -c "ALTER TABLE %s ADD CONSTRAINT enforce_srid_the_geom CHECK (srid(the_geom) = 2193);" """ % (name,))

    indexes = [
        "create index idx_mb%(y)s_au%(y)s on mb%(y)s (au%(y)s)" % {'y' : year},
        "create index idx_mb%(y)s_ua%(y)s on mb%(y)s (ua%(y)s)" % {'y' : year},
        "create index idx_mb%(y)s_ta%(y)s on mb%(y)s (ta%(y)s)" % {'y' : year},
        "create index idx_mb%(y)s_regc%(y)s on mb%(y)s (regc%(y)s)" % {'y' : year},
        "create index idx_mb%(y)s_the_geom on mb%(y)s using gist (the_geom gist_geometry_ops)" % {'y' : year},
    ]
    for index in indexes:
        os.system("""psql -d cdes -c "%s" """ % (index,))
    
if __name__ == '__main__':
    main()
