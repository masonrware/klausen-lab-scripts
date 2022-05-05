#!usr/bin/python3.9

# j6.py
# Version 1.0.0
# 5/2/2022

# Western Jihadism Project; Brandeis University
# Written By: Mason Ware


''' '''


import os
import csv
import json
from typing import Dict
import pandas as pd     #type: ignore

from utils.animate import Loader


class File:
    ''' Class to represent a file of csv data and all of its attributes. '''
    def __init__(self, file_path: str) -> None:
        # j6 var sheet:
        self.db_domestic_extremist_infile_csv: str = file_path
        self.db_domestic_extremist_outfile_csv: str = 'data/csv/out_j6_var_sheet.csv'
        self.db_domestic_extremist_outfile_json: str = 'data/json/out_j6_var_sheet.json'
        
        # person-independent:
        self.db_city_outfile_csv: str = 'data/csv/out_db_city.csv'
        self.db_city_outfile_json: str = 'data/json/out_db_city.json'
        self.c_rad_outfile_csv: str = 'data/csv/out_rad.csv'
        self.c_rad_outfile_json: str = 'data/json/out_rad.json'
        self.db_ethnicity_outfile_csv: str = 'data/csv/out_db_ethnicity.csv'
        self.db_ethnicity_outfile_json: str = 'data/json/out_db_ethnicity.json'
        
        # person-dependent:
        self.db_de_arrest_outfile_csv: str = 'data/csv/out_db_de_arrest.csv'
        self.db_de_arrest_outfile_json: str = 'data/json/out_db_de_arrest.json'
        self.db_de_residency_outfile_csv: str = 'data/csv/out_db_de_residency.csv'
        self.db_de_residency_outfile_json: str = 'data/json/out_db_de_residency.json'
        self.db_de_citizenship_outfile_csv: str = 'data/csv/out_db_de_citizenship.csv'
        self.db_de_citizenship_outfile_json: str = 'data/json/out_db_de_citizenship.json'
        self.db_de_personalias_outfile_csv: str = 'data/csv/out_db_de_personalias.csv'
        self.db_de_personalias_outfile_json: str = 'data/json/out_db_de_personalias.json'
        self.db_personalias_outfile_csv: str = 'data/csv/out_db_personalias.csv'
        self.db_personalias_outfile_json: str = 'data/json/out_db_personalias.json'
        
        # j6 var sheet:
        self.j6_var_sheet_data: list(dict()) = list(dict())
        
        # person-independent:
        self.db_city_hometown_out: list(dict()) = list()
        self.db_city_death_out: list(dict()) = list()
        self.c_rad_out: list(dict()) = list()
        self.db_ethnicity_out: list(dict()) = list()

        # person-dependent:       
        self.db_de_arrest_out: list(dict()) = list()
        self.db_de_residency_out: list(dict()) = list()
        self.db_de_citizenship_out: list(dict()) = list()
        self.db_de_personalias_out: list(dict()) = list()
        
    def load_dataframe(self) -> None:
        ''' method to load the csv file into a dataframe. '''
        # collect cleaned json data from raw csv
        with open(self.db_domestic_extremist_infile_csv, 'r', encoding='UTF-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            j: int = 7884
            for i, rows in enumerate(csv_reader):
                # cleaning
                if not rows['Hometown']:
                    rows['Hometown'] = ","
            
                self.j6_var_sheet_data.append({
                    'id': j+1,
                    'legacy_id': 'DE' + str(i+1),
                    'legacy_id_num': i+1,
                    'person_name': rows['Name'],
                    'nickname': rows['Alias'],
                    'terrorist_type': 'JANUARY 6',                  # ?
                    'criminal_type': 'JANUARY 6',            # ?
                    'year_first_terror_le_contact': rows['Year of First Terror-Related Law Enforcement Contact'],
                    'year_born': rows['Year Born'],
                    'sex': rows['Sex'],

                    'db_ethnicity_ethinicity_name': rows['Ethnicity'],
                    # 'ethnicity_id': {
                    #     # 'id': j+1,
                    #     'ethnicity_name': rows['Ethnicity']
                    # },
                    'immigration_status': rows['Immigration Status'],
                    'native_born': rows['Native Born'],
                    'naturalized': None,
                    'asylum_seeker': None,
                    'education': rows['Education'],
                    'profression': rows['Profession'], 
                    'employed': rows['Employed'],
                    'suicide_status': None,
                    'year_radicalization': rows['Year Radicalized'],
                    'west_residence_id': None,
                    'db_city_hometown_city': rows['Hometown'].split(',')[0],
                    'db_city_hometown_region': rows['Hometown'].split(',')[1],
                    'db_city_hometown_country_id': 840,
                    # 'hometown_id': {
                    #     # 'id': i+1,
                    #     'city': rows['Hometown'].split(',')[0],
                    #     'region': rows['Hometown'].split(',')[1],
                    #     'country_id': 840,
                    # },                                                          # this needs to be sent to db_city
                    'year_death': rows['Year of Death'],
                    'cause_death': rows['Cause of Death'],

                    'db_city_death_city': rows['City of Death'].split(',')[0],
                    'db_city_death_region': None,
                    'db_city_death_country_id': 840,
                    # 'city_death_id': {
                    #     # 'id': i+1,
                    #     'city': rows['City of Death'].split(',')[0],
                    #     'region': None,
                    #     'country_id': 840, 
                    # },                                                          # this needs to be sent to db_city
                    'country_of_death': 840,
                    'status': '',
                    'plot': '',
                    'demographics': '',
                    'edu_occ': None,
                    'religion': None,
                    'core_ideology': None,
                    'foreign_ties': None,
                    'activity': None,
                    'philosophy': None,
                    'sources': '',
                    'recent_status': rows['Recent Status'],
                    'last_updated': '2022-05-05',                                        # unsure about date - what is the specific type
                    'mugshot_file': None,
                    'flec_reason': rows['FLEC Reason'],

                    'c_radicalization_reason': rows['Radicalization Reason'],
                    'c_radicalization_description': None,
                    # 'radicalization_reason_id': {
                    #     'id': i+1,
                    #     'reason': rows['Radicalization Reason'],
                    #     'description': None
                    # },                                                          # this needs to be sent to c_radicalization
                    'nonviolent_action': None,         # ?  
                    'violent_action': None,            # ?
                    'dropout': None,                   # ?
                    'social_behavioral': rows['Social-Behavioral'],
                    'domestic_extremist': None,
                    # below are dicts that will be cut out to go elsewhere
                    'db_de_arrest': {
                        'arrest_year': rows['Date of J6 Arrest'].split('-')[0],
                        'arrest_country_id': 840,
                        'arrest_outcome': None,
                        'sentence_year': None,
                        'sentence_country_id': None,
                        'sentence_type': None,
                        'sentence_length': None,
                        'life_sentence': None,
                        'death_sentence': None,
                        'de_person_id': i+1
                    },
                    'db_de_citizenship': {
                        # 'id': i+1,
                        'citizenship_type': 'PRIMARY',
                        'country_id': 840,
                        'de_person_id': i+1
                    },
                    'db_de_residency': {
                        # 'id': i+1,
                        'residency_type': 'PRIMARY',
                        'country_id': 840,
                        'de_person_id': i+1
                    },
                    'db_de_personalias': {
                        # 'id': i+1,
                        'person_alias': rows['Alias'],
                        'de_person_id': i+1
                    },                                                          # this needs to be sent to db_personalalias
                })
                j+=1
        
    def handle_endpoints(self) -> None:
        ''' '''
        for entry in self.j6_var_sheet_data:
            # person-independent:
            # self.db_city_hometown_out.append(entry['hometown_id'])
            # #? entry['hometown_id'] = id?
            # self.db_city_death_out.append(entry['city_death_id'])
            # #? entry['city_death_id'] = id?
            # self.c_rad_out.append(entry['radicalization_reason_id'])
            # #? entry['radicalization_reason_id'] = id?
            # self.db_ethnicity_out.append(entry['ethnicity_id'])
            # #? entry['ethnicity_id'] = id?
            
            # person-dependent:
            self.db_de_arrest_out.append(entry['db_de_arrest'])
            del entry['db_de_arrest']
            self.db_de_residency_out.append(entry['db_de_residency'])
            del entry['db_de_residency']
            self.db_de_citizenship_out.append(entry['db_de_citizenship'])
            del entry['db_de_citizenship']
            self.db_de_personalias_out.append(entry['db_de_personalias'])
            del entry['db_de_personalias']
        
    def write_out(self) -> None:
        ''' '''        
        # main output:

        # j6 variable sheet --> db_domestic_extremist
        # json
        with open(self.db_domestic_extremist_outfile_json, 'w') as json_file:
            json_file.write(json.dumps(self.j6_var_sheet_data, indent=4)) 
        # csv
        keys = self.j6_var_sheet_data[0].keys()
        with open(self.db_domestic_extremist_outfile_csv, 'w', newline=None) as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.j6_var_sheet_data)
        
        # person-independent outputs:
        
        # # hometown --> db_city
        # with open(self.db_city_outfile_json, 'w') as json_file:
        #     json_file.write(json.dumps(self.db_city_hometown_out, indent=4))
        # keys = self.db_city_hometown_out[0].keys()
        # with open(self.db_city_outfile_csv, 'w', newline=None) as output_file:
        #     dict_writer = csv.DictWriter(output_file, keys)
        #     dict_writer.writeheader()
        #     dict_writer.writerows(self.db_city_hometown_out) 
              
        # # death-town --> db_city
        # with open(self.db_city_outfile_json, 'w') as json_file:
        #     json_file.write(json.dumps(self.db_city_death_out, indent=4))
        # with open(self.db_city_outfile_csv, 'a', newline=None) as output_file:
        #     dict_writer = csv.DictWriter(output_file, keys)
        #     dict_writer.writerows(self.db_city_death_out)
            
        # # radicalization --> c_radicalization
        # with open(self.c_rad_outfile_json, 'w') as json_file:
        #     json_file.write(json.dumps(self.c_rad_out, indent=4))
        # keys = self.c_rad_out[0].keys()
        # with open(self.c_rad_outfile_csv, 'w', newline=None) as output_file:
        #     dict_writer = csv.DictWriter(output_file, keys)
        #     dict_writer.writeheader()
        #     dict_writer.writerows(self.c_rad_out)  
            
        # # ethnicity --> db_ethnicity
        # with open(self.db_ethnicity_outfile_json, 'w') as json_file:
        #     json_file.write(json.dumps(self.db_ethnicity_out, indent=4))
        # keys = self.db_ethnicity_out[0].keys()
        # with open(self.db_ethnicity_outfile_csv, 'w', newline=None) as output_file:
        #     dict_writer = csv.DictWriter(output_file, keys)
        #     dict_writer.writeheader()
        #     dict_writer.writerows(self.db_ethnicity_out)  
        
        # person-dependent outputs:
        
        # arrest --> db_de_arrest
        with open(self.db_de_arrest_outfile_json, 'w') as json_file:
            json_file.write(json.dumps(self.db_de_arrest_out, indent=4))
        keys = self.db_de_arrest_out[0].keys()
        with open(self.db_de_arrest_outfile_csv, 'w', newline=None) as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.db_de_arrest_out) 
        
        # residency --> db_de_residency
        with open(self.db_de_residency_outfile_json, 'w') as json_file:
            json_file.write(json.dumps(self.db_de_residency_out, indent=4))
        keys = self.db_de_residency_out[0].keys()
        with open(self.db_de_residency_outfile_csv, 'w', newline=None) as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.db_de_residency_out) 
            
        # citizenship --> db_de_citizenship
        with open(self.db_de_citizenship_outfile_json, 'w') as json_file:
            json_file.write(json.dumps(self.db_de_citizenship_out, indent=4))
        keys = self.db_de_citizenship_out[0].keys()
        with open(self.db_de_citizenship_outfile_csv, 'w', newline=None) as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.db_de_citizenship_out)
            
        # alias --> personalias  (a copy is also sent to db_de under 'nickname')
        with open(self.db_de_personalias_outfile_json, 'w') as json_file:
            json_file.write(json.dumps(self.db_de_personalias_out, indent=4))
        keys = self.db_de_personalias_out[0].keys()
        with open(self.db_de_personalias_outfile_csv, 'w', newline=None) as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.db_de_personalias_out) 
      
      
def main() -> None:
    # user_io_file = input(f"\nEnter a file path > ")                                           
    user_io_file = "data/csv/in_j6_var_sheet.csv"                                                 
    user_file = File(file_path=user_io_file)
    user_file.load_dataframe()
    user_file.handle_endpoints()
    user_file.write_out()
    
if __name__ == '__main__':
    main()