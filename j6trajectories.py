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
        self.rt_demoprof_infile_csv: str = file_path
        
        # person-dependent:
        self.rt_demoprof_outfile_csv: str = 'data/csv/out_rt_de_demographicprofile.csv'
        self.rt_demoprof_outfile_json: str = 'data/csv/out_rt_de_demographicprofile.json'
        self.rt_indicator_outfile_csv: str = 'data/csv/out_indicator.csv'
        self.rt_indicator_outfile_json: str = 'data/json/out_indicator.csv'
        
        # person-dependent:       
        self.db_de_rt_demographic_profile_out: list(dict()) = list()
        self.db_de_rt_indicator_out: list(dict()) = list()
        
    
    def load_dataframe(self) -> None:
        ''' method to load the csv file into a dataframe. '''
        # collect cleaned json data from raw csv
        with open(self.rt_demoprof_infile_csv, 'r', encoding='UTF-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for i, rows in enumerate(csv_reader):
                # TODO
                # add info to indicator
                # ^ keep a dict for demoprof in the traj_id?
                pass
                
                
        
    def handle_endpoints(self) -> None:
        ''' '''
        # TODO
        # move the traj_id dict out and replace it with an id
        for entry in self.j6_var_sheet_data:
            # person-independent:
            self.db_city_hometown_out.append(entry['hometown_id'])
            #? entry['hometown_id'] = id?
            self.db_city_death_out.append(entry['city_death_id'])
            #? entry['city_death_id'] = id?
            self.c_rad_out.append(entry['radicalization_reason_id'])
            #? entry['radicalization_reason_id'] = id?
            self.db_ethnicity_out.append(entry['ethnicity_id'])
            #? entry['ethnicity_id'] = id?
            
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
        
        # hometown --> db_city
        with open(self.db_city_outfile_json, 'w') as json_file:
            json_file.write(json.dumps(self.db_city_hometown_out, indent=4))
        keys = self.db_city_hometown_out[0].keys()
        with open(self.db_city_outfile_csv, 'w', newline=None) as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.db_city_hometown_out) 
              
        # death-town --> db_city
        with open(self.db_city_outfile_json, 'w') as json_file:
            json_file.write(json.dumps(self.db_city_death_out, indent=4))
        with open(self.db_city_outfile_csv, 'a', newline=None) as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writerows(self.db_city_death_out)
            
        # radicalization --> c_radicalization
        with open(self.c_rad_outfile_json, 'w') as json_file:
            json_file.write(json.dumps(self.c_rad_out, indent=4))
        keys = self.c_rad_out[0].keys()
        with open(self.c_rad_outfile_csv, 'w', newline=None) as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.c_rad_out)  
            
        # ethnicity --> db_ethnicity
        with open(self.db_ethnicity_outfile_json, 'w') as json_file:
            json_file.write(json.dumps(self.db_ethnicity_out, indent=4))
        keys = self.db_ethnicity_out[0].keys()
        with open(self.db_ethnicity_outfile_csv, 'w', newline=None) as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.db_ethnicity_out)  
        
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