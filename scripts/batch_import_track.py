"""
Run import_track.py using all tracks found with a glob-style search string

Usage:
    batch_import_track.py <connection_txt> <search_str> [--seg_time_diff=<int>] [--min_point_distance=<int>] [--registration=<str>] [--ssl_cert_path=<str>] [--submission_method=<str>] [--operator_code=<str>] [--aircraft_type=<str>] [--walk_dir_tree]
    batch_import_track.py <connection_txt> --show_operators

Examples:
    python batch_import_track.py ..\..\connection_info.txt "P:\Aviation - DENA\FY19 Aviation\Fleet\Flight Tracks\02.February\**\*" -r N21HY -o NPS

Required parameters:
    connection_txt      Path of a text file containing information to connect to the DB. Each line
                        in the text file must be in the form 'variable_name; variable_value.'
                        Required variables: username, password, ip_address, port, db_name.
    search_str          Either a glob-style pattern or directory path to traverse (if --walk_dir_tree option
                        given) to find tracks to import

Options:
    -h, --help                      Show this screen.
    --seg_time_diff=<int>           Minimum time in minutes between two points in a track file indicating the start of
                                    a new track segment [default: 15]
    -d, --min_point_distance=<int>  Minimum distance in meters between consecutive track points to determine unique
                                    vertices. Any points that are less than this distance from and have the same
                                    timestamp as the preceding point will be removed. [default: 200]
    -r, --registration=<str>        Tail (N-) number of the aircraft. Note that supplying this assumes all track files
                                    are from the same aircraft.
    -o, --operator_code=<str>       Three digit code for the operator of the aircraft. All administrative flights
                                    should submitted with the code NPS
    -c, --ssl_cert_path=<str>       Path to an SSL .crt or .pem file for sending an HTTP request to registry.faa.gov to
                                    retrieve info about the aircraft
    -t, --aircraft_type=<str>       The model name of the aircraft
    -f, --force_import              If specified, import all data even if there are matching flight segments
                                    in the database already
    -s, --show_operators            Print all available operator names and codes to the console
    -w, --walk_dir_tree             Search for files in all sub-dirs of the directory specified by search_str. Only
                                    files with a recognizable file extension (.gdb, .gpx, or .csv) will be processed

"""

import sys, os
import re
import glob
import subprocess
from datetime import datetime
import import_track


def main(connection_txt, search_str, seg_time_diff=15, min_point_distance=200, operator_code=None, aircraft_type=None, registration=None, walk_dir_tree=False, force_import=False, ssl_cert_path=None):

    subprocess.call('', shell=True) #For some reason, this enables ANSII escape characters to be properly read by cmd.exe

    if walk_dir_tree:
        if not os.path.isdir(search_str):
            raise ValueError('If --walk_dir_tree or -w option specified, search_str must be an existing directory with '
                             'track files in it. %s was given' % search_str)
        track_paths = []
        for root, dirs,  files in os.walk(search_str):
            track_paths.extend([os.path.join(root, f) for f in files if os.path.splitext(f)[1] in import_track.READ_FUNCTIONS])
        if not len(track_paths):
            raise ValueError('No tracks found searching the search_str directory %s. Track files must end in one of the'
                             ' following extensions: %s' % (search_str, ', '.join(import_track.READ_FUNCTIONS.keys()))
                             )
    else:
        track_paths = glob.glob(search_str, recursive=True)
        if not len(track_paths):
            raise ValueError('No tracks found with the search_str %s. Is this a directory that you meant to use with '
                             '--walk_dir_tree? For help, try python batch_import_track.py --help' % search_str)

    failed_tracks = {}
    n_tracks = len(track_paths)
    for i, path in enumerate(track_paths):
        # Show progress (\r returns to the start of the current line and \033[K clears it)
        sys.stdout.write('\r\033[KProcessing {path} | {this_n:d} of {n_tracks:d} ({percent:.1f}%)'
                         .format(path=os.path.basename(path), this_n=i + 1, n_tracks=n_tracks, percent=float(i + 1)/n_tracks * 100))
        
        try:
            import_track.import_track(connection_txt,
                                      path,
                                      seg_time_diff=seg_time_diff,
                                      min_point_distance=min_point_distance,
                                      registration=registration,
                                      submission_method='manual',
                                      operator_code=operator_code,
                                      aircraft_type=aircraft_type,
                                      force_import=force_import,
                                      ssl_cert_path=ssl_cert_path)

        except Exception as e:
            failed_tracks[path] = e

    n_failed = len(failed_tracks)
    if n_failed:
        failed_track_str = '\n\t-'.join(['%s: %s' % t for t in failed_tracks.items()])
        print('\n\nAll tracks imported successfully except:\n\t-%s' % failed_track_str)
    else:
        print('\n\nAll tracks successfully imported')


if __name__ == '__main__':
    args = import_track.get_cl_args(__doc__)

    if args['show_operators']:
        sys.exit(import_track.print_operator_codes(args['connection_txt']))
    else:
        del args['show_operators']
        sys.exit(main(**args))
