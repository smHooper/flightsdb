"""
Find saved submissions (SQLite .db files)

Usage:
    find_submissions.py <submitter> [--submission_type=<str>] [--search_dir=<str>] [--start_date=<str>] [--end_date=<str>] [--output_csv=<str>] [--no_interactive]

Examples:
    find_submissions.py fly_denali --submission_type=tracks --no_interactive
    find_submissions.py fly_denali -t tracks -n

Required parameters:
    submitter                  The submitter to search for

Options:
    -h, --help              how this screen

    -d, --search_dir=<str> 	Directory to search in

    -e, --end_date=<str>	Latest submission date to look for files.
                            Dates must be in YYYY-mm-dd format

    -n, --no_interactive    Disable interactive mode. When enabled,
                            interactive mode causes the script to stop
                            after each found submission and confirm
                          	with the user whether or not to continue

	-o, --output_csv=<str>	Path of the CSV to write results to. The CSV will
							have the contents of the 'flights' table with a 
							file column added giving the path to each file found
    
    -s, --start_date=<str>  Earliest submission date to look for files.
                            Dates must be in YYYY-mm-dd format

    -t, --submission_type=<str>
                            Type of submission: tracks or landings	

"""

from datetime import datetime
from glob import glob
import os
import pandas as pd
from sqlalchemy import create_engine
import sys
from utils import get_cl_args

SEARCH_DIR = r'\\inpdenaterm01\overflights\poll_feature_service_logs'
SEARCH_PATTERN = 'dena_flight_data_query_view_*.db'
DEFAULT_START_DATE = '1970-1-1'
DEFAULT_END_DATE = datetime.now().strftime('%Y-%m-%d')

def main(
		submitter: str, 
		submission_type: str='', 
		search_dir: str='', 
		interactive: bool=True,
		start_date: str='',
		end_date: str='',
		output_csv: str='submissions.csv'
	) -> None:
	# File names all have timestamps in them and, therefore, sort temporally 
	#	when sorted alphabetically. Show in descending order to get most recent 
	#	first
	db_paths = glob(os.path.join(search_dir or SEARCH_DIR, SEARCH_PATTERN))[::-1] 
	
	if start_date or end_date:
		try:
			start_date = datetime.strptime(
				start_date or DEFAULT_START_DATE, 
				'%Y-%m-%d'
			).date()
		except:
			raise ValueError(
				'start_date "{start_date}" is not a valid date'
				' the format YYYY-mm-dd'
			)
		try:
			end_date = datetime.strptime(
				end_date or DEFAULT_END_DATE, 
				'%Y-%m-%d'
			).date()
		except:
			raise ValueError(
				'end_date "{end_date}" is not a valid date' 
				'in the format YYYY-mm-dd'
			)

		format_str = SEARCH_PATTERN.replace('*', '%Y%m%d-%H%M%S')
		dbs = pd.DataFrame({
			'path': db_paths,
			'timestamp': [
					datetime.strptime(os.path.basename(p), format_str).date()
					for p in db_paths
				]
			})
		#import pdb; pdb.set_trace()
		db_paths = dbs.loc[
			(dbs.timestamp > start_date) & 
			(dbs.timestamp <= end_date)
		].path.to_list()

	# Create the SQL to query DBs
	submission_type_where = (
		f'''AND submission_type = '{submission_type}' '''
		if submission_type 
		else ''
	)
	sql = f'''
		SELECT * FROM flights 
		WHERE (
			tracks_operator = '{submitter}' OR	
			landing_operator = '{submitter}' OR
			creator = '{submitter}'	
		) 
		{submission_type_where}
	'''

	flight_list = []
	for db_path_ in db_paths: 
		engine = create_engine('sqlite:///' + db_path_)
		flights = pd.read_sql(sql, engine)
		flights['file'] = db_path_
		if len(flights):
			flight_list.append(flights)

		if interactive:
			print(db_path_)
			response = input('Show the next file?')
			if not response.lower().startswith('y'):
				print(f'\nExiting... {len(flight_list)} files found')
				return

	all_flights = pd.concat(flight_list)
	if output_csv:
		all_flights['submission_time'] = [
			datetime.fromtimestamp(t / 1000)
			for t in all_flights.CreationDate
		]
		try:
			all_flights.to_csv(output_csv, index=False)
		except:
			raise IOError(f'output_csv not value: {output_csv}')

	print(f'\n{len(all_flights)} submissions found in {len(flight_list)} files')


if __name__ == '__main__':
    args = get_cl_args(__doc__)
    # Set the 'interactive' arg to the opposite of whatever the value of 
    #	negation flag 'no_interactive' is. If the flag was passed, the value in
    #	the args dict will be True, naking interactive=False. If it wasn't 
    #	passed, .get() returns '', the opposite of which is True.
    args['interactive'] = not args.get('no_interactive')
    # Remove it since it's not an actual argument
    del args['no_interactive']
    
    sys.exit(main(**args))
