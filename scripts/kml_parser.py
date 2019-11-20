import os
import xml.sax


class KMLParser(xml.sax.ContentHandler):
    '''
    Adapted from https://gist.github.com/timabell/8791116

    Retrieve an OGR readable GPX string like
    gpx_string = KMLParser().parse(kml_file_path)

    Convert the KML directly to a gpx file like
    KMLParser().to_gpx(kml_path, gpx_path)
    
    '''

    def __init__(self):
        self.in_tag = 0
        self.chars = ""
        self.when = ""
        self.started = 0
        self._gpx_string = \
            '<?xml version="1.0" encoding="UTF-8"?>\n' \
            '<gpx version="1.0"\n' \
                '\txmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n' \
                '\tcreator="%s (adapted from https://gist.github.com/timabell/8791116)"\n' % __file__ +\
                '\txmlns="http://www.topografix.com/GPX/1/0"\n' \
                '\txsi:schemaLocation="http://www.topografix.com/GPX/1/0 http://www.topografix.com/GPX/1/0/gpx.xsd">\n' \
            '\t<trk>\n' \
            '\t\t<trkseg>\n'

    def startElement(self, name, attrs):
        if name == "gx:coord":
            self.in_tag = 1
            self.chars = ""
        if name == "when":
            self.in_tag = 1
            self.chars = ""
        if name == "gx:Track" and self.started:
            self._gpx_string += '\t\t</trkseg>\n'
            self._gpx_string += '\t\t<trkseg>\n'


    def characters(self, char):
        if self.in_tag:
            self.chars += char


    def endElement(self, name):
        if name == "when":
            self.in_coord=0
            self.when = self.chars
            self.chars=""
        if name == "gx:coord":
            self.in_coord = 0
            self.started = 1
            coords=self.chars
            self.chars = ""
            coords = coords.split()
            self._gpx_string += '\t\t\t<trkpt lat="%s" lon="%s">\n' % (coords[1],coords[0])
            if len(coords) > 2:
                self._gpx_string += '\t\t\t\t<ele>%s</ele>\n' % coords[2]
            self._gpx_string += '\t\t\t\t<time>%s</time>\n' % self.when
            self._gpx_string += "\t\t\t</trkpt>\n"


    def close_gpx(self):
        self._gpx_string += '\t\t</trkseg>\n\t</trk>\n</gpx>\n'


    def parse(self, f):
        xml.sax.parse(f, self)
        self.close_gpx()

        return self._gpx_string


    def to_gpx(self, kml_path, gpx_path=None):
        self.parse(kml_path)
        if not gpx_path:
            _, extension = os.path.splitext(kml_path)
            gpx_path = kml_path.rstrip(extension) + '.gpx'
            if os.path.isfile(gpx_path):
                raise IOError('default gpx_path already exists: %s. Specify a different path to write the GPX to' % gpx_path)
        with open(gpx_path, 'w') as gpx:
            gpx.write(self._gpx_string)

        return gpx_path