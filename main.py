import requests
from bs4 import BeautifulSoup
import datetime as dt
import pandas as pd
from multiprocessing.pool import ThreadPool
from itertools import repeat
import os

#=======================================================================================================================

def navigate_to_sports_page(URL):
	#TODO website failed to reach error
	site_data = BeautifulSoup(requests.get(URL).text, 'lxml')

	sports_home = [i for i in site_data.find_all('a') if i.text.lower() == 'sports'][0]
	#TODO Indexing error
	sports_home = sports_home.attrs['href']
	site_data = BeautifulSoup(requests.get(sports_home).text, 'lxml')

	#TODO class name may change
	#fetch the overarching seasonal divs
	sports_pages = site_data.find('div', {'class': 'su-tabs-panes'})
	sports_pages = [i for i in sports_pages.children if not isinstance(i, str)]
	#TODO length not equal to 3 error

	return sports_pages

#-----------------------------------------------------------------------------------------------------------------------

def initialize_sport_dict(sports_pages):
	# Fetches and organizes each sport page and url
	sport_dict = {}
	compounded_tuples = []
	for page in sports_pages:
		sport_tuples = []
		season = page.attrs['data-title']
		sport_rows = page.find_all('div', {'class': 'su-note'})
		for sport in sport_rows:
			url = None
			descendants = [i for i in sport.descendants if i != '\n']
			name = [i for i in descendants if isinstance(i, str)][0]
			for descendant in [i for i in descendants if not isinstance(i, str)]:
				attrs = descendant.attrs
				if 'href' in attrs:
					url = attrs['href']
					break
			sport_tuples.append((name, url))

		sport_dict[season] = []
		for sport, _ in sport_tuples:
			sport_dict[season].append(sport)

		compounded_tuples += sport_tuples

	return sport_dict, compounded_tuples

#-----------------------------------------------------------------------------------------------------------------------

def initialize_school_dataframes(sport_dict, compounded_tuples, YEAR_CUTOFF):

	# ------------------------------------------------------------------------------------------------------------------

	def scrape(sport_tuple, df_dict, YEAR_CUTOFF):
		sport, url = sport_tuple
		print(f'Fetching {sport} ({url})')

		site = requests.get(url)
		if not site.ok:
			print(f'Failed to fetch site for {sport} ({url})')
		else:
			site = BeautifulSoup(site.text, 'lxml')
			tab_panes = [i for i in site.find('div', {'class': 'su-tabs-panes'}).children if i != '\n']
			for pane in tab_panes:
				attrs = pane.attrs
				if 'data-title' in attrs:
					if 'tournament' in attrs['data-title'].lower():
						break

			tournament_results = pane.find_all('tr')
			headers = [i.text.lower() for i in tournament_results.pop(0).contents]

			tournament_df = []
			year = None
			for result in tournament_results:
				contents = [i.text.strip() for i in result.contents]
				year_placeholder = contents[0]
				# If new year, store the variable. Otherwise, replace blank year with previous year
				if year_placeholder:
					year = int(year_placeholder)
					if year <= YEAR_CUTOFF:
						break
				else:
					contents[0] = year
				tournament_df.append(dict(zip(headers, contents)))

			df_dict[sport] = pd.DataFrame(tournament_df)

	# ------------------------------------------------------------------------------------------------------------------

	df_dict = {name: None for (name, _) in compounded_tuples}
	with ThreadPool() as tp:
		tp.starmap(scrape, zip(compounded_tuples, repeat(df_dict), repeat(YEAR_CUTOFF)))

	# isolate the start of the latest season
	fall_sport = sport_dict['Fall'][0]
	fall_year = int(df_dict[fall_sport]['year'].iloc[0])

	school_dict = {}
	for sport, df in df_dict.items():
		df['sport'] = sport
		df['year'] = df['year'].astype('int')
		empty_df = pd.DataFrame(columns=list(df))

		if sport in sport_dict['Fall']:
			df = df[df['year'] == fall_year]
		else:
			df = df[df['year'] > fall_year]

		schools = [i.strip() for i in set(df['champion']) if i.strip()]
		# ignore individual golf champions
		cleaned_schools = []
		for school in schools:
			if 'Boys:' in school or 'Girls:' in school:
				continue
			else:
				cleaned_schools.append(school)

		for school in cleaned_schools:
			# if school not yet entered, and it is an individual school name and not a co-champion, add to dictionary
			if school not in school_dict:
				if '/' in school:
					co_champions = [i.strip() for i in school.split('/')]
					for co_champion in co_champions:
						if co_champion not in school_dict:
							school_dict[co_champion] = empty_df
				else:
					school_dict[school] = empty_df

			# assign co-champions to their individual schools
			if '/' in school:
				for co_champion in co_champions:
					school_dict[co_champion] = pd.concat(
						(school_dict[co_champion], df[df['champion'] == f'{school}']))
			else:
				school_dict[school] = pd.concat(
					(school_dict[school], df[df['champion'] == f'{school}']))

	return school_dict

#-----------------------------------------------------------------------------------------------------------------------

def write_file(school_dict):
	wins_df = []
	for school, df in school_dict.items():
		temp_dict = {'school': school, 'wins': df.shape[0]}
		wins_df.append(temp_dict)
	wins_df = pd.DataFrame(wins_df).sort_values(by='wins', ascending=False).reset_index(drop=True)
	wins_string = wins_df.to_string(header=False, index=False)

	DROP_COLUMNS = ['details', 'score']
	# initialize main body variable for final output
	output_body = ''
	# variable for list of schools in order of their wins
	ordered_schools = list(wins_df['school'])
	# iterate through schools in order and fetch the string versions of their data tables
	for school in ordered_schools:
		table_string = '-'*120 + '\n' + '{:5s}   {:22s}   {:22s}   {:30s}   {:22s}\n'.format('YEAR', 'CHAMPION', 'SPORT', 'CLASS', 'RUNNERUP',)
		school_df = school_dict[school].drop(columns=DROP_COLUMNS).sort_values(by='year', ascending=False)\
			.to_dict('records')
		for row in school_df:
			year, class_, champion, runnerup, sport = [str(i) for i in row.values()]
			table_string += '{:5s} | {:22s} | {:22s} | {:30s} | {:22s}\n'.format(year, champion, sport, class_, runnerup)

		output_body += table_string + '-'*120 + '\n\n\n'

	content = f'{wins_string}\n\n\n{output_body}'
	output_path = os.path.join(os.path.dirname(__file__), 'deliverable.txt')
	with open(output_path, 'w') as w:
		w.write(content)
	print(f'\nFinished writing file to {output_path}')
	input('Press Enter to close\n>> ')

#-----------------------------------------------------------------------------------------------------------------------

def main(YEAR_CUTOFF, URL):
	sport_dict, compounded_tuples = initialize_sport_dict(navigate_to_sports_page(URL))
	write_file(initialize_school_dataframes(sport_dict, compounded_tuples, YEAR_CUTOFF))

#=======================================================================================================================

if __name__ == '__main__':
	YEAR_CUTOFF = dt.datetime.today().year - 2
	URL = r'http://ciacsports.com/site/'

	main(YEAR_CUTOFF, URL)
