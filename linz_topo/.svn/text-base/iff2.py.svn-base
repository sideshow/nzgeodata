#!/usr/bin/env python
#
# Copyright 2008-2010 Koordinates Limited
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os
import csv
import ogr
import osr
from decimal import Inf
from optparse import OptionParser
import shutil

# Layers to process when we're in debug mode
DEBUG_MAXLINES = 20000000   # don't process more input lines than this

USAGE="""
Convert the LINZ Topo data from the LAMPS2 .iff files as distributed by LINZ 
into Shapefiles.

Usage:
    %prog [options] (--shapefile DEST)|(--postgis DBSTR) IFF...
    
Each IFF can be a path to either the raw .iff or the compressed .iff.Z file.
File extensions are not case-sensitive.

Use either --shapefile and specify an output directory DEST, or --postgis
and specify an OGR DB connection string.
"""

def main():
    parser = OptionParser(usage=USAGE)
    parser.add_option("-S", "--srid", dest="srid", type="int", default=2193,
                      help="SRID to use for the map data. Details should have been registered with GDAL. [default: %default]")
    parser.add_option("-D", "--debug", dest="debug", action="store_true", default=False,
                      help="Debug mode: test only specified layers, and don't process every line. [default: %default]")
    parser.add_option("--debug-layers", dest="debug_layers", action="append", default=[320],
                      help="When in debug mode, the layers to process. [default: mine_poly]")
    parser.add_option("-p", "--postgis", dest="postgis", default=None,
                      help="Output to PostGIS DB. Specify connection string as \"dbname='dbname' host='host' user='x' password='x' port='5432'\"")
    parser.add_option("-s", "--shapefile", dest="shapefile", default=None,
                      help="Output ESRI Shapefile to specified directory, which will be cleared.")
    (opts, iffs) = parser.parse_args()
    
    if not len(iffs):
        parser.error("You must specify at least one .iff file")
    
    if (opts.postgis and opts.shapefile) or not (opts.postgis or opts.shapefile):
        parser.error("Specify one of SHP or PostGIS output")
    
    if opts.shapefile:
        if os.path.exists(opts.shapefile):
            shutil.rmtree(opts.shapefile)
        opts.shapefile = os.path.abspath(opts.shapefile)
        os.makedirs(opts.shapefile)
        
        ds_drv = "ESRI Shapefile"
        ds_str = opts.shapefile
        lc_opts = []
    else:
        ds_drv = "PostgreSQL"
        ds_str = 'PG:%s' % opts.postgis
        lc_opts = ['DIM=2', 'LAUNDER=NO', 'OVERWRITE=YES']
    
    processor = IFFProcessor(debug=opts.debug, srid=opts.srid)
    for iff in iffs:
        if not os.path.exists(iff):
            parser.error("File does not exist: %s" % iff)
        print "IFF file:", iff
        processor.run(iff, ds_drv, ds_str, lc_opts)
    
    print "Finished!"

class IFFProcessor(object):
    def __init__(self, srid=2193, debug=False, debug_layers=None):
        self.srid = srid
        self.debug = debug
        self.debug_layers = debug_layers
        
        scriptfolder = os.path.split(__file__)[0]
        self.attributes = {}
        reader = csv.reader(open(os.path.join(scriptfolder,"ACcodes.csv"), 'r'))
        for code, desc in reader:
            self.attributes[code] = desc
        
        self.tables = {}
        reader = csv.reader(open(os.path.join(scriptfolder,"FScodes.csv"), 'r'))
        for code, desc in reader:
            self.tables[code] = desc
        
        self.srs = osr.SpatialReference()
        self.srs.ImportFromEPSG(self.srid)
        assert self.srs, "Could not create spatial reference from SRID %d" % self.srid
    
    GEOMETRY_TYPES = {
        'bdy': ogr.wkbLineString,
        'cl': ogr.wkbLineString,
        'poly': ogr.wkbPolygon, #polygons can have multiple rings, see build_geometry() below
        'seed': ogr.wkbPoint,
        'pnt': ogr.wkbPoint,
        'edg': ogr.wkbLineString,
        'edge': ogr.wkbLineString,
        
        # some tables don't have a suffix to denote geometry type. 
        # most of them (see the TEXT theme) appear to be POINT geometries. Exceptions go here:
        'contour': ogr.wkbLineString,
        'coastline': ogr.wkbLineString,
        'line': ogr.wkbLineString,
    }
    
    WKT_BASES = {
        ogr.wkbLineString: 'LINESTRING(%s)',
        ogr.wkbPoint: 'POINT(%s)',
        ogr.wkbPolygon: 'POLYGON(%s%s)',
    }
    
    def run(self, iff, driver_name, ds_desc, layer_opts):
        fileObj = self.get_file(iff)
        
        ogr_driver = ogr.GetDriverByName(driver_name)
        
        dataset = ogr_driver.CreateDataSource(ds_desc)
        assert dataset is not None, "Error creating datasource: %s: %s" % (driver_name, ds_desc)
        
        layers = {}
        layer_fields = {}
        attribute_data_types = {}
        
        print "Importing from %s" % iff
        
        feature_serial = None
        feature_id = None
        feature_code_int = None
        feature_code_text = None
        points = []
        fids = {}
        geometry_field_name = 'the_geom' 
        first_geometry = True
        
        linecount = 0
        printstep = 20000
        
        skip_feature = False
        
        print "\nPass 1 of 2: Detecting attribute names and types..."
        for line in fileObj:
            linecount += 1
            if linecount % printstep == 0:
                print linecount
            code = line[:2]
            
            if self.debug and linecount >= DEBUG_MAXLINES:
                print "DEBUG is on, stopping 1st pass at %d lines." % DEBUG_MAXLINES
                break
            
            #for debugging purposes        
            if skip_feature and code != 'FS':
                continue
            
            if code == 'FS':
                # new feature
                feature_code_int = line[3:].strip().split(" ", 1)[0]
                
                if self.debug:
                    skip_feature = feature_code_int not in self.debug_layers
                    if skip_feature:
                        continue
                
                table_name = self.tables[feature_code_int]
                geometry_type = self.get_geometry_type(table_name)
                
                if table_name not in layers:
                    print "Creating layer: %s" % table_name
                    layers[table_name] = dataset.CreateLayer(table_name, self.srs, geometry_type, options=layer_opts)
                    assert layers[table_name] is not None, "Error creating layer: %s" % table_name
                    
                    layer_fields[table_name] = []
                    fids[table_name] = 1
                
                layer = layers[table_name]
            
            elif code == 'AC':
                # attribute
                int_type, numeric_value = line[3:].strip().split(" ")[0:2]
                try:
                    textual_value = line[3:].strip().split(" ", 2)[2]
                except IndexError, e:
                    textual_value = None
                
                if table_name not in layer_fields:
                    continue
                
                attribute_name = self.get_attribute_name(int_type)
                if attribute_name not in layer_fields[table_name]:
                    layer_fields[table_name].append(attribute_name)
                    attribute_data_types[attribute_name] = ogr.OFTReal
                if (textual_value != '""') and (textual_value is not None) and (attribute_data_types[attribute_name], ogr.OFTReal):
                    attribute_data_types[attribute_name] = ogr.OFTString

        for table_name in layer_fields.keys():
            for attribute_name in layer_fields[table_name]:
                field_def = ogr.FieldDefn(attribute_name, attribute_data_types[attribute_name])
                assert (0 == layers[table_name].CreateField(field_def)), "Creating field failed: %s" % attribute_name
        linecount = 0

        fileObj.close()
        if os.path.splitext(iff)[1].lower() == '.z':
            print "Decompressing", iff
            fileObj = os.popen('uncompress -c "%s"' % iff, 'r')
        else:
            fileObj = open(iff, 'r')

        skip_feature = False

        points = []
        sections = []

        print "\nPass 2 of 2: Importing features."
        for line in fileObj:
            linecount += 1
            if linecount % printstep == 0:
                print linecount
            code = line[:2]

            #for debugging purposes        
            if skip_feature and code not in ('FS', 'NF'):
                continue

            if code in ('RA', 'HI', 'NS', 'NO', 'RO', 'EO', 'EM', 'EJ'):
                #not implemented yet, or we don't care
                continue
            elif code == 'NF':
                feature_serial, feature_id = line[3:].strip().split(" ")
                attribute_values = {}
                first_geometry = True
            elif code == 'FS':
                feature_code_int = line[3:].strip().split(" ", 1)[0]

                if self.debug:
                    skip_feature = feature_code_int not in self.debug_layers
                    if skip_feature:
                        continue

                table_name = self.tables[feature_code_int]
                geometry_type = self.get_geometry_type(table_name)

                layer = layers[table_name]

            elif code == 'AC':
                int_type, numeric_value = line[3:].strip().split(" ")[0:2]
                try:
                    textual_value = line[3:].strip().split(" ", 2)[2]
                except IndexError, e:
                    textual_value = None

                attribute_name = self.get_attribute_name(int_type)
                if (textual_value is None) or (attribute_data_types[attribute_name] == ogr.OFTReal):
                    attribute_values[attribute_name] = numeric_value
                else:
                    if textual_value:
                        textual_value = textual_value[1:-1]  # (remove quotes)
                    attribute_values[attribute_name] = textual_value

            elif code == 'ST':
                if line[3:].strip()[-1] == '0' and not first_geometry:
                    sections.append(points)
                    points = []
                first_geometry = False
            elif code == 'EF':
                if feature_code_int == '29':
                    pass
                sections.append(points)
                try:
                    geom = self.build_geometry(geometry_type, sections)
                except ValueError, e:
                    print "IFF line:", linecount
                    raise
                self.insert_row(layers[table_name], geom, attribute_values, fids[table_name])
                fids[table_name] += 1
                sections = []
                points = []
            else:
                #point data
                point = [float(z) for z in line.strip().split(" ")]

                points.append(point)

        fileObj.close()

        for i in range(dataset.GetLayerCount()):
            ogr_layer = dataset.GetLayer(i)
            dataset.ExecuteSQL("CREATE SPATIAL INDEX ON %s" % ogr_layer.GetName())
            ogr_layer.SyncToDisk()
        dataset.Destroy()
    
    def get_file(self, path):
        if os.path.splitext(path)[1].lower() == '.z':
            print "Decompressing", path
            return os.popen('uncompress -c "%s"' % path, 'r', 8192)
        else:
            return open(path, 'r')
    
    _ignored_geometry_types = set()
    def get_geometry_type(self, table_name):
        try:
            gt = self.GEOMETRY_TYPES[table_name.split("_")[-1]]
        except KeyError, e:
            gt = ogr.wkbPoint
            if table_name not in self._ignored_geometry_types:
                print >>sys.stderr, "WARNING: unknown geometry type for table %s, assuming type POINT." % (table_name,)
                self._ignored_geometry_types.add(table_name)
        return gt
    
    def get_attribute_name(self, int_type):
        try:
            return self.attributes[int_type]
        except KeyError, e:
            if not hasattr(self, '_unknown_attrs'):
                self._unknown_attrs = set()
            if int_type not in self._unknown_attrs:
                self._unknown_attrs.add(int_type)
                print >>sys.stderr, "WARNING: Unknown attribute %s" % int_type
            return "a%d" % int(int_type)
    
    def build_geometry(self, geometry_type, sections):
        if geometry_type == ogr.wkbPolygon:
            
            #Sections are lists of points which were separated in the input data by 'ST [numpoints] 0' lines.
            # Each section in a polygon feature is either the start or end of a ring.
            # Rings are nested inside each other for some reason, like so:
            #
            # Section 0:       Ring 0
            #   Section 1:     Ring 1
            #     Section 2:   Ring 2
            #   Section 3:     Ring 1
            # Section 4:       Ring 0
            
            if len(sections) % 2 != 1:
                raise Exception("len(sections) (%d) for a polygon feature is not odd!" % len(sections))
            rings = []
            for i in range(len(sections)/2):
                ring = sections[i]
                ring.extend(sections[-1 -i])
                rings.append(ring)
            rings.append(sections[len(sections)/2])
            
            #check for duplicated points. These form a self-touching ring which needs to be
            # split off to form a hole. Note that the order of the rings is arbitrary in
            # the input data so we also need to determine which is the exterior ring (see below)
            r = 0
            while r < len(rings):
                ring = rings[r]
                #ignore first point (not using a slice since that would duplicate the point list)
                #obviously the first point and the last point will be the same, but we don't care
                firstpoint = ring.pop(0)
                i = 1
                while i < len(ring):
                    point = ring[i]
                    index = ring.index(point)
                    if index < i - 1:
                        #duplicate point found, create a new ring for the points between the duplicates
                        new_ring = ring[index:i+1]
                        rings.append(new_ring)
                        #remove all points between the duplicates from the first ring
                        for z in range(i - index):
                            ring.pop(index)
                    i += 1
                ring.insert(0, firstpoint)
                r += 1
            
            # Need to determine which is the exterior ring.
            # (i.e. the ring with the most extreme point values...)
            extremes = [-Inf, -Inf, Inf, Inf]
            exterior_ring_index = 0
            for i in range(len(rings)):
                xs = [point[0] for point in rings[i]]
                ys = [point[1] for point in rings[i]]
                for direction in range(0,4):
                    func = (direction >=2) and max or min
                    greater = direction >=2 and -1 or 1
                    if i == 0 or greater * cmp(func(direction % 2 == 0 and xs or ys), extremes[direction]) > 0:
                        exterior_ring_index = i
                        extremes = [max(xs), max(ys), min(xs), min(ys)]
                        break
            
            exterior_ring = rings[exterior_ring_index]
            rings.pop(exterior_ring_index)
            
            stringified_rings = []
            for ring in rings:
                stringified_rings.append(',(%s)' % ",".join([" ".join([str(z) for z in point]) for point in ring]))
            
            stringified_exterior_ring = '(%s)' % ",".join([" ".join([str(z) for z in point]) for point in exterior_ring])
            
            wkt = self.WKT_BASES[geometry_type] % (stringified_exterior_ring, ''.join(stringified_rings))
        
        else:
            all_points = []
            for section in sections:
                all_points.extend(section)
                
            wkt = self.WKT_BASES[geometry_type] % ",".join([" ".join([str(z) for z in point]) for point in all_points])
        
        try:
            return ogr.CreateGeometryFromWkt(wkt, self.srs)
        except ValueError, e:
            print "Error creating geometry from text:\n", wkt
            raise
    
    def insert_row(self, layer, geom, attribute_values, fid):
        ogr_feature = ogr.Feature(layer.GetLayerDefn())
        ogr_feature.SetFID(fid)
        ogr_feature.SetGeometryDirectly(geom)
        
        attribute_values = attribute_values.copy()
        for k,v in attribute_values.items():
            ogr_feature.SetField(k, v)
        
        code = layer.CreateFeature(ogr_feature)
        if code != 0:
            raise ValueError("Could not create feature (code=%d)" % code)
        
        ogr_feature.Destroy()

if __name__ == '__main__':
    main()
