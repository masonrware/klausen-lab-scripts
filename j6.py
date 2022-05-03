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
        self.in_file_path: str = file_path
        self.in_json_path: str = 'data/json/in_j6_var_sheet.json'
        self.out_json_path: str = 'data/json/out_j6_var_sheet.json'
        self.out_csv_path: str = 'data/csv/out_j6_var_sheet.csv'
        
        self.in_file_data: list(dict()) = list(dict())
        self.out_file_data_temp: list(dict()) = list(dict())
        self.out_file_data: list(dict()) = list(dict())
        
    
    def load_dataframe(self) -> None:
        ''' method to load the csv file into a dataframe. '''
        # collect cleaned json data from raw csv
        with open(self.in_file_path, 'r', encoding='UTF-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for i, rows in enumerate(csv_reader):
                # TODO
                # in the future, I will replace those that need to be sent somewhere with
                # a dict that reps the schema in their destination. That way I can just have
                # a method go through and send them to new files as well as replace them
                # with a (retrieved?) id
                self.in_file_data.append({
                    'id': i+1,
                    'legacy_id': 'DE' + str(i+1),
                    'legacy_id_num': i+1,
                    'person_name': rows['Name'],
                    'nickname': rows['alias'],
                    'alias': {
                        'id': i+1,
                        'person_alias': rows['alias'],
                        'person_id': i+1
                    },                                              # this needs to be sent to db_personalalias
                    # 'terrorist_type': rows[],                     ?
                    # 'criminal_type': rows[],                      ?
                    'year_first_terror_le_contact': rows['Year of First Terror-Related Law Enforcement Contact'],
                    'year_born': rows['Year Born'],
                    'sex': rows['Sex'],
                    'ethnicity_id': rows['Ethnicity'],
                    'immigration_status': rows['Immigration Status'],
                    'native_born': rows['Native Born'],
                    'education': rows['Education'],
                    'profression': rows['Profession'], 
                    'employed': rows['Employed'],
                    'year_radicalization': rows['Year Radicalized'],
                    'hometown_id': {
                        'id': i+1,
                        'city': rows['Hometown'].split(',')[0],
                        'region': rows['Hometown'].split(',')[1],
                        'country_id': '', #! country code for USA
                    },                                              # this needs to be sent to db_city
                    'year_death': rows['Year of Death'],
                    'cause_death': rows['Cause of Death'],
                    'city_of_death': {
                        'id': i+1,
                        'city': rows['City of Death'].split(',')[0],
                        'region': '',
                        'country_id': '', #! country code for USA
                    },                                              # this needs to be sent to db_city
                    'country_of_death': '', #! code for US          # this needs to be sent to db_country
                    'status': '',
                    'plot': '',
                    'demographics': '',
                    'edu_occ': '',
                    'core_ideology': '',
                    'foreign_ties': '',
                    'activity': '',
                    'sources': '',
                    'recent_status': rows['Recent Status'],
                    'last_updated': 20220502,                       # unsure about date - what is the specific type
                    'mugshot_file': '',
                    'flec_reason': rows['FLEC Reason'],
                    'radicalization_reason_id': {
                        'id': i+1,
                        'reason': rows['Radicalization Reason'],
                        'description': ''
                    }                                               # this needs to be sent to c_radicalization
                    # 'nonviolent_action':,         ?  
                    # 'violent_action':,            ?
                    # 'dropout':,                   ?
                    'social_behavioral': rows['Social-Behavioral']
                })
        # write to json
        with open(self.in_json_path, 'w') as json_file:
            json_file.write(json.dumps(self.in_file_data, indent=4))
        
    def handle_endpoints(self) -> None:
        db_personalias_outfile_csv: str = 'data/csv/out_db_personalias.csv'
        db_personalias_outfile_json: str = 'data/json/out_db_personalias.json'
        db_city_hometown_outfile_csv: str = 'data/csv/out_db_city_hometown.csv'
        db_city_hometown_outfile_json: str = 'data/json/out_db_city_hometown.json'
        db_city_death_outfile_csv: str = 'data/csv/out_db_death.csv'
        db_city_death_outfile_json: str = 'data/json/out_db_death.json'
        r_rad_outfile_csv: str = 'data/csv/out_rad.csv'
        r_rad_outfile_json: str = 'data/json/out_rad.json'
        
        db_personalias_dict: list(dict())
        db_city_hometown_dict: list(dict())
        db_city_death_dict: list(dict())
        c_rad_dict: list(dict())
        
        # go through and send the dicts to their endpoints
        # ? recieving id somehow?
        for entry in self.in_file_data:
            db_personalias_dict.append(entry['alias'])
            db_city_death_dict.append(entry['city_of_death'])
            db_city_hometown_dict.append(entry['hometown_id'])
            c_rad_dict.append(entry['radicalization_reason_id'])
        
        # write db personalias to file and replace with id
        
        
        
        # clean all the data in self.in_file_data
        # iterate through 
    
    def clean_output(self) -> None:
        j = 0
        for dict in self.out_file_data_temp:
            links = dict['link_s'].split(';')
            if len(links) > 1:
                for i in range(len(links)-1):
                    res = {'id': j,
                           'link_s': links[i],
                           'person1_id': dict['person1_id'],
                           'person2_id': dict['person2_id']
                            }
                    j+=1
                    self.out_file_data.append(res)
            else:
                res = {'id': j,
                       'link_s': links[0],
                       'person1_id': dict['person1_id'],
                           'person2_id': dict['person2_id']
                        }
                j+=1
                self.out_file_data.append(res)
                
        
    def write_out(self) -> None:
        ''' method to write processed csv to a new json file and new csv file. '''
        # write to json
        with open(self.out_json_path, 'w') as json_file:
            json_file.write(json.dumps(self.out_file_data, indent=4))
            
        # for some reason, this writes to csvs and makes lists with multiple items in them raw strs?
        # print(self.out_file_data)
        keys = self.out_file_data[0].keys()
        with open(self.out_csv_path, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.out_file_data)
  
      
def main() -> None:
    # user_io_file = input(f"\nEnter a file path > ")                                           # for user input
    user_io_file = "data/csv/in_j6_var_sheet.csv"                                                 # for file
    user_file = File(file_path=user_io_file)
    user_file.load_dataframe()
    # user_file.parse_linkages()
    # user_file.clean_output()
    # user_file.write_out()
    
if __name__ == '__main__':
    main()