# Ethan McIntosh - GIS and Data Services - Brown University - April 1, 2022

import os
import requests
import sys
import pandas as pd
import urllib.parse

parent_folder = os.path.dirname(os.getcwd())
file_path = parent_folder + '\\stolen-relations-data-export.csv'
sr = pd.read_csv(file_path)

# ----- geonames stuff -------
base_url = r'http://api.geonames.org/searchJSON?'
uname = REDACTED

# replace misread characters with their literals, and ; with , for simplicity
sr['Location'] = sr['Location'].str.replace('[APOS]', '\'', regex=False)
sr['Location'] = sr['Location'].str.replace('[QUOT]', '\"', regex=False)
sr['Location'] = sr['Location'].str.replace(';', ',', regex=False)
sr['Location'] = sr['Location'].fillna("")
# sr[['City', 'Subregion', 'Country', 'State', 'NoMatch1', 'NoMatch2', 'rs']] = ''
sr[['lat', 'lng', 'from']] = ''


# ----------- HELPER METHODS FOR DATA INITIALIZATION ------- #

# constructs dictionaries mapping place names to lat-long coordinate tuples
def coords_dict(df):
    return dict(zip(df[0], zip(df[1], df[2])))


# constructs dictionaries mapping place names to tuples of (latitude, longitude,
# country code, region code)
def coords_and_more_dict(df):
    return dict(zip(df[0], zip(df[1], df[2], df[3], df[4])))


# ------------ DATA INITIALIZATION ---------------- #

# This csv stores information about place names the script has already searched
# in a dictionary.  In addition to the main dictionary (searched_names), I
# initialize nested dictionaries of state- and country-specific dictionaries.

srchd = pd.read_csv('searched.csv', encoding='utf-8-sig', header=None)
searched_names = coords_and_more_dict(srchd)
searched_countries = {}
for ct in list(srchd[3].value_counts().keys()):
    srchd_ctys = srchd[srchd[3] == ct]
    searched_countries[ct] = coords_and_more_dict(srchd_ctys)

searched_states = {}
us_srchd = srchd[srchd[3] == 'US']
for st in list(us_srchd[4].value_counts().keys()):
    us_srchd_sts = us_srchd[us_srchd[4] == st]
    searched_states[st] = coords_and_more_dict(us_srchd_sts)

# These csvs store coordinate and other info for world countries, US states,
# US counties, and world cities, respectively, as name-keyed dictionaries.
# Additional nested dictionaries are made for counties and cities by US state as
# well as cities by (non-US) country.

cts = pd.read_csv('countries.csv', encoding='utf-8-sig', header=None)
countries = coords_dict(cts)
country_abbrs = dict(zip(cts[0], cts[3]))

sts = pd.read_csv('states.csv', encoding='utf-8-sig', header=None)
states = coords_dict(sts)
state_abbrs = dict(zip(sts[0], sts[3]))

counties = pd.read_csv('counties.csv', encoding='utf-8-sig', header=None)
state_counties = {}
for st in state_abbrs.values():  # st are two letter abbreviations
    st_cntys = counties[counties[3] == st]
    state_counties[st] = coords_dict(st_cntys)

cities = pd.read_csv('cities.csv', encoding='utf-8-sig', header=None)
state_cities = {}
us_cities = cities[cities[3] == 'US']
for st in state_abbrs.keys():  # st are full state names
    st_ctys = us_cities[us_cities[4] == st]
    state_cities[state_abbrs[st]] = coords_dict(st_ctys)

country_cities = {}
non_us_cities = cities[cities[3] != 'US']
for ct in country_abbrs.keys():
    ct_ctys = non_us_cities[non_us_cities[3] == country_abbrs[ct]]
    country_cities[country_abbrs[ct]] = coords_dict(ct_ctys)


# -------------- HELPER METHODS FOR EXECUTION ----------- #

# performs HTTP request on GeoNames API and returns the top result as a tuple of
# (latitude, longitude, country code, region code)
def search_geonames(place_name, search_params):
    query = urllib.parse.quote(place_name)  # converts string to URL format
    extra = ''
    for param in search_params.keys():
        extra += '&' + param + '=' + search_params[param]
    data_url = f'{base_url}q={query}&username={uname}&maxRows=10{extra}'
    response = requests.get(data_url)

    if 'status' in response.json():  # if there's an exception with the request
        exit_processing()  # saves what the script has processed so far
        print(response.json()['status']['message'])
        sys.exit()

    gn = response.json()['geonames']  # successful request
    if gn:  # if search has results
        index = 0
        result = gn[index]
        components = ['lat', 'lng', 'countryCode', 'adminCode1']
        while not all(cmpts in result for cmpts in components):
            index += 1
            result = gn[index]
        return (result['lat'], result['lng'], result['countryCode'],
                result['adminCode1'])
    else:  # if search returned no results
        return ('', '', '', '')  # need an empty literal to store empty results


# returns either the main searched_names dictionaries or one of the nested
# ones specific to a state or country, based on search_params.
def get_searched_dict(search_params):
    if search_params['country'] == 'US':
        if search_params['adminCode1'] not in searched_states:
            searched_states[search_params['adminCode1']] = {}
        return searched_states[search_params['adminCode1']]
    elif search_params['country'] != '':
        if search_params['country'] not in searched_countries:
            searched_countries[search_params['country']] = {}
        return searched_countries[search_params['country']]
    else:
        return searched_names


# update the cells at a given row (indexed at r) for latitude, longitude, and
# the component of the place name from which those coordinates were derived
def set_lat_long_from(r, lat, lng, fr):
    sr.at[r, 'lat'] = lat
    sr.at[r, 'lng'] = lng
    sr.at[r, 'from'] = fr


# return a list of strings corresponding to the comma-separated components of
# a place name (or a list with just the place name if it has no commas)
def comma_list(str):
    if ',' in str:
        return str.split(',')
    else:
        return [str]


# identifies previously searched results that are empty or null
def is_empty_tuple(t):
    return t == ('', '', '', '') \
           or t[0] != t[0]   # only true for null values


# identifies whether a search_params dictionary of indeterminate size is empty
def is_empty_params(search_params):
    for val in search_params.values():
        if val != '':
            return False
    return True


# saves both the dictionary of searched names and the updated data table as csvs
def exit_processing():
    unzipped = list(zip(*searched_names.values()))
    searched = pd.DataFrame({
        'name': list(searched_names.keys()),
        'lat': unzipped[0],
        'lng': unzipped[1],
        'country': unzipped[2],
        'state': unzipped[3]
        })
    searched.to_csv('searched.csv', header=None, index=False, encoding='utf-8-sig')
    print("Number of new searches performed: " + str(gn_searches))
    sr.to_csv('test_out.csv', encoding='utf-8-sig', index=False)


# ----------------- EXECUTION ----------------- #

# This section has a while loop (iterating through components of a place name)
# within a for loop (iterating through all the place names in the data table)

gn_searches = 0  # keeps track of how many GeoNames requests we make
for r in range(len(sr)):
    # variables that help us navigate the while loop for a single place name
    names = comma_list(sr.at[r, 'Location'])  # components of a single placename
    go_to_next_name = True
    next_dict = states
    search_params = {'country': '', 'adminCode1': ''}

    # print out progress updates every time the script completes 100 rows
    if r in range(100, len(sr), 100):
        print(str(r) + "/" + str(len(sr)) + " rows complete. " +
              str(gn_searches) + " searches performed.")

    # loop continues while there remain place name components to be processed
    while names or (not names and not go_to_next_name):
        if go_to_next_name:
            # remove the next component of the place name, from the end
            name = names.pop(-1).strip()

        if len(name) <= 1:  # if we get stray characters like - instead of names
            go_to_next_name = True
            continue  # proceed to next iteration of loop without searching

        # this if block is unwieldy, but it follows a specific logic. We first
        # check if a place name component is a US state, then a country, then
        # a previously searched name.  If a place name component is a US state
        # or country, we set next_dict to be a country- or state-specific
        # dictionary, so that we search those dicts on the next loop iteration
        if next_dict == searched_names:
            searched = get_searched_dict(search_params)
            if name in searched:  # we've searched for [name] before
                # if our previous search for [name] got no results,
                if is_empty_tuple(searched[name]):
                    go_to_next_name = True
                    continue  # proceed to the next component of the place name.

                # if we currently have no search parameters, set GeoNames
                # countryBias parameter to favor results in [name]'s country
                if is_empty_params(search_params):
                    search_params['countryBias'] = searched[name][2]

                # store the coordinates from the previous search in the table
                set_lat_long_from(r, searched[name][0], searched[name][1], name)
                go_to_next_name = True

            # if we've searched for [name] before, but not in the country- or
            # state-specific dictionary we need to be in...
            elif name in searched_names:
                go_to_next_name = True
                continue  # ...proceed to the next component of the place name.

            else:  # i.e., we haven't searched for [name] before
                geonames = search_geonames(name, search_params)  # search GNames
                searched_names[name] = geonames  # store result, even if empty
                gn_searches += 1

                if not is_empty_tuple(geonames):  # if the result was not empty,
                    # store the coordinates from this search into the table
                    set_lat_long_from(r, geonames[0], geonames[1], name)

                    # store this result into the corresponding country- or
                    # state-specific dictionary
                    if geonames[2] == 'US':
                        if geonames[3] not in searched_states:
                            searched_states[geonames[3]] = {}
                        searched_states[geonames[3]][name] = geonames
                    else:
                        if geonames[2] not in searched_countries:
                            searched_countries[geonames[2]] = {}
                        searched_countries[geonames[2]][name] = geonames

                go_to_next_name = True  # proceed to next component of placename

        elif next_dict == states:
            if name in states:  # if [name] is one of the 50 US states

                # store the coordinates for this state into the table
                set_lat_long_from(r, states[name][0], states[name][1], name)

                # ensure that GeoNames searches for other components of this
                # place name are filtered to results within this US state
                search_params['country'] = 'US'
                search_params['adminCode1'] = state_abbrs[name]

                # set this state's counties as the next dictionary to search
                next_dict = state_counties[state_abbrs[name]]
                go_to_next_name = True
            else:
                go_to_next_name = False  # don't proceed to next component...
                next_dict = countries  # ... see if it's a country first.
        elif next_dict == countries:
            if name in countries:  # if [name] is one of the world's countries

                # store the coordinates for this country into the table
                set_lat_long_from(r, countries[name][0], countries[name][1], name)

                # ensure that GeoNames searches for other components of this
                # place name are filtered to results within this country
                search_params['country'] = country_abbrs[name]

                # set this country's cities as the next dictionary to search
                next_dict = country_cities[country_abbrs[name]]
                go_to_next_name = True
            else:  # if [name] is not a country,
                go_to_next_name = False  # don't proceed to next component...
                next_dict = searched_names  # ... see if we've searched it before

        # if we're in a state-specific dictionary of counties
        elif list(next_dict.keys()) and 'County' in list(next_dict.keys())[0]:
            if name in next_dict:
                set_lat_long_from(r, next_dict[name][0], next_dict[name][1], name)
                go_to_next_name = True
            else:
                go_to_next_name = False

            # either way, the next dictionary to search is this state's cities
            next_dict = state_cities[search_params['adminCode1']]
        else:  # i.e., we're in a country- or state-specific dict of cities
            if name in next_dict:  # if [name] is a known city,

                # store the coordinates for this country into the table...
                set_lat_long_from(r, next_dict[name][0], next_dict[name][1], name)

                # ...and don't bother searching the rest of the place name
                # components.  City-level precision is good enough.
                break
            else:
                go_to_next_name = False

            # the next dictionary to search is searched_names
            next_dict = searched_names

# Phew, made it through all the rows of the data table!  All done.
exit_processing()
