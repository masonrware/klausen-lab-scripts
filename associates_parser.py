#!usr/bin/python3.9

# parser.py
# Version 1.0.0
# 4/8/2022

# Western Jihadism Project; Brandeis University
# Written By: Mason Ware


''' This is a python module to process and identify attributes of a csv file of associations
to be entered into the laboratory database. It is understood that the new linkages are entered
by respective encoders and not to a template exactly - hence the need for this module in place
of simply using regex or egrep. '''


import os
import csv
import json
import pandas as pd     #type: ignore


class File:
    ''' Class to represent a file of csv data and all of its attributes. '''
    def __init__(self, file_path: str) -> None:
        self.in_file_path: str = file_path
        self.in_json_path: str = 'data/in_associates_2.0.json'
        self.out_json_path: str = 'data/out_associates_2.0.json'
        self.out_csv_path: str = 'data/out_associates_2.0.csv'
        
        self.in_file_data: list(dict()) = list(dict())
        self.out_file_data_temp: list(dict()) = list(dict())
        self.out_file_data: list(dict()) = list(dict())
        self.regex_links: dict() = {
            'regex_1d_links': {
                'COORDINATOR OF': ['coordinat'],
                'FINANCIAL/LOGISITC SUPPORTOR OF': ['financial', 'logistic'],
                'LEADER OF': ['leader'],
                'RECRUITER OF': ['recruiter'],
                'SOCIAL MEDIA FOLLOWER': ['social media follower'],
                'SPIRITUAL LEADER OF': ['spiritual'],
                'TEACHER OF': ['teacher'],
                'TRANSACTION': ['transaction'],
                'VISITOR OF': ['visitor', 'visitors']
            },
            'regex_Md_links': {
                'ASSOCIATE OF': ['associate'],
                'CHILDHOOD FRIEND OF': ['childhood'],
                'COLLEAGUE OF': ['colleague'],
                'COMMUNICATION': ['communication'],
                'EMPLOYER OF': ['employ'],
                'FRIEND OF': ['friend'],
                'HOUSEMATE OF': ['housemate'],
                'MEETING': ['meeting'],
                'SOCIAL MEDIA CONTACT': ['social media contact'],
                'SHARED PLOT': ['shared', 'plot'],
                'TRAVEL': ['travel']
            },
            'regex_kin_links': {
                'COUSIN OF': ['cousin'],
                'IN-LAW OF': ['in-law'],
                'KIN OF': ['kin'],
                'PARENT OF': ['parent'],
                'SIBILING OF': ['sibiling'],
                'SPOUSE OF': ['spouse'],
                'UNCLE/AUNT OF': ['uncle', 'aunt']
            }
        }
    
    def load_dataframe(self) -> None:
        ''' method to load the csv file into a dataframe. '''
        # collect cleaned json data from raw csv
        with open(self.in_file_path, 'r', encoding='UTF-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for rows in csv_reader:
                self.in_file_data.append({
                    'person1_legacy_id': rows['Person 1 Legacy ID'],
                    'person2_legacy_id': rows['Person 2 Legacy ID'],
                    'person1_id': rows['person1_id'],
                    'person2_id': rows['person2_id'],
                    'old_link': rows['link_type'],
                    'RA': rows['RA'],
                    'new_link': rows['Link Type'],
                    'comments': rows['Comments']
                })
        # write to json
        with open(self.in_json_path, 'w') as json_file:
            json_file.write(json.dumps(self.in_file_data, indent=4))
        
    def parse_linkages(self) -> None:
        ''' method to process each linkage in the csv table from raw english. '''
        with open(self.in_json_path, 'r', encoding='UTF-8') as file:
            json_object = json.load(file)
            for association in json_object:
                    res = {'link_s': str(),
                           'person1_legacy_id': association['person1_legacy_id'],
                           'person2_legacy_id': association['person2_legacy_id'],
                           }
                    if association["new_link"]:
                        new_link_s = association["new_link"].split(';')                             # separate each link in human annotation
                        for link_str in new_link_s:                                                 # for each link in one to potentially many links
                            for regex_link_group in self.regex_links:                               # for each set of links
                                for regex_link_type in self.regex_links[regex_link_group]:          # for each link in a set of links
                                    if any(token in link_str.lower() for token in self.regex_links[regex_link_group][regex_link_type]):  
                                        res['link_s'] += regex_link_type + ';'
                    if not res['link_s']:
                        res['link_s'] += 'ASSOCIATE OF'
                    self.out_file_data_temp.append(res)
    
    def clean_output(self) -> None:
        j = 0
        for dict in self.out_file_data_temp:
            links = dict['link_s'].split(';')
            if len(links) > 1:
                for i in range(len(links)-1):
                    res = {'id': j,
                           'link_s': links[i],
                           'person1_legacy_id': dict['person1_legacy_id'],
                            'person2_legacy_id': dict['person2_legacy_id']
                            }
                    j+=1
                    self.out_file_data.append(res)
            else:
                res = {'id': j,
                       'link_s': links[0],
                       'person1_legacy_id': dict['person1_legacy_id'],
                        'person2_legacy_id': dict['person2_legacy_id']
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
    user_io_file = "data/in_associates_2.0.csv"                                                 # for file
    user_file = File(file_path=user_io_file)
    user_file.load_dataframe()
    user_file.parse_linkages()
    user_file.clean_output()
    user_file.write_out()
    
if __name__ == '__main__':
    main()