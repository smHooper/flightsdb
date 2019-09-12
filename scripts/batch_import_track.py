"""
Run import_track.py using all tracks found with a glob-style search string

Usage:
    batch_import_track.py <connection_txt> <search_str> [--seg_time_diff=<int>] [--min_point_distance=<int>] [--registration=<str>] [--submission_method=<str>] [--operator_code=<str>] [--aircraft_type=<str>]
    batch_import_track.py <connection_txt> --show_operators

Examples:


Required parameters:
    connection_txt      Path of a text file containing information to connect to the DB. Each line
                        in the text file must be in the form 'variable_name; variable_value.'
                        Required variables: username, password, ip_address, port, db_name.
    search_str          Glob-style pattern to find tracks to import

Options:
    -h, --help                      Show this screen.
    --seg_time_diff=<int>           Minimum time in minutes between two points in a track file indicating the start of
                                    a new track segment [default: 15]
    -d, --min_point_distance=<int>  Minimum distance in meters between consecutive track points to determine unique
                                    vertices. Any points that are less than this distance from the preceeding point will
                                    be removed. [default: 500]
    -r, --registration=<str>        Tail (N-) number of the aircraft
    -o, --operator_code=<str>       Three digit code for the operator of the aircraft. All administrative flights
                                    should submitted with the code NPS
    -t, --aircraft_type=<str>       The model name of the aircraft
    -s, --show_operators            Print all available operator names and codes to the console

"""

import sys, os
import re
import glob
import subprocess
from datetime import datetime
import time
import import_track


def main(connection_txt, search_str, seg_time_diff=15, min_point_distance=500, operator_code=None, aircraft_type=None, registration=None):

    subprocess.call('', shell=True) #For some reason, this enables ANSII escape characters to be properly read by cmd.exe

    failed_tracks = {}
    track_paths = glob.glob(search_str)
    n_tracks = len(track_paths)
    for i, path in enumerate(track_paths):

        sys.stdout.write('\r\033[KProcessing {path} | {this_n:d} of {n_tracks:d} ({percent:.1f}%)'
                         .format(path=os.path.basename(path), this_n=i + 1, n_tracks=n_tracks, percent=float(i + 1)/n_tracks * 100))
        #sys.stdout.flush()

        if not registration:
            reg_matches = re.findall(r'(?i)N\d{2,5}[A-Z]{0,2}', os.path.basename(path))
            registration = reg_matches[0] if len(reg_matches) else ''
        
        try:
            import_track.import_track(connection_txt,
                                      path,
                                      seg_time_diff=seg_time_diff,
                                      min_point_distance=min_point_distance,
                                      registration=registration,
                                      submission_method='manual',
                                      operator_code=operator_code,
                                      aircraft_type=aircraft_type)


        except Exception as e:
            failed_tracks[path] = e

    n_failed = len(failed_tracks)
    if n_failed:
        print('\n\nAll tracks imported successfully except:\n')


if __name__ == '__main__':
    args = import_track.get_cl_args(__doc__)

    if args['show_operators']:
        sys.exit(import_track.print_operator_codes(args['connection_txt']))
    else:
        del args['show_operators']
        sys.exit(main(**args))
