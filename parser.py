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
import re
import pandas as pd     #type: ignore

class File:
    ''' Class to represent a file of csv data and all of its attributes. '''
    def __init__(self, file_path: str) -> None:
        self.file_path: str = file_path
        self.file_data: "pd.DataFrame"
        
        self.regex_1d_links: dict() = {
            'COORDINATOR_OF': ['coordinator', 'of'],
            'FINANCIAL_LOGISITC_SUPPORTOR_OF': ['financial/logistical', 'supporter', 'of'],
            'LEADER_OF': ['leader', 'of'],
            'RECRUITER_OF_SPONSOR_OF': ['recruiter', 'of'],
            'SOCIAL_FOLLOWER': ['social', 'media', 'follower'],
            'SPIRITUAL_LEADER_OF': ['spiritual', 'leader', 'of'],
            'TEACHER_OF': ['teacher', 'of'],
            'TRANSACTION': ['transaction'],
            'VISITOR_OF': ['visitor', 'of']
        }
        self.regex_Md_links: dict() = {
            'ASSOCIATE_OF': ['associate', 'of'],
            'CHILDHOOD_FRIEND': ['childhood', 'friend', 'of'],
            'COLLEAGUE_OF': ['colleague', 'of'],
            'COMMUNICATION': ['communication'],
            'EMPLOYER_OF': ['employer', 'of'],
            'FRIEND_OF': ['friend', 'of'],
            'HOUSEMATE_OF': ['housemate', 'of'],
            'MEETING': ['meeting'],
            'SOCIAL_CONTACT': ['social', 'media', 'contact'],
            'SHARED_PLOT': ['shared', 'plot'],
            'TRAVEL': ['travel']
        }
        self.regex_kin_links: dict() = {
            'COUSIN_OF': ['cousin', 'of'],
            'INLAW_OF': ['in', '-', 'law', 'of'],
            'KIN_OF': ['kin', 'of'],
            'PARENT_OF': ['parent', 'of'],
            'SIBILING_OF': ['sibiling', 'of'],
            'SPOUSE_OF': ['spouse', 'of'],
            'UNCLE_AUNT_OF': ['uncle', 'aunt', 'of']
        }
    
    def load_dataframe(self) -> None:
        ''' method to load the csv file into a dataframe. '''
        with open(self.file_path, 'r', encoding='UTF-8') as csv_file:
            temp_file_data = pd.read_csv(csv_file)
        self.file_data = temp_file_data
        
    def parse_linkage(self) -> None:
        ''' method to process each linkage in the csv table from raw english. '''
        for line in self.file_data:
            if line["Link Type"]:
                # go through each linkage and see if the regex is there collectively
                # * might have to see results of ^ and then decide if I need to look for individually
                
                # if it has a link type ...
                pass
        

def main() -> None:
    # user_io_file = input(f"\nEnter a file path > ")           # for user input
    user_io_file = "data/Associates_2.0.csv"                    # for file
    user_file = File(file_path=user_io_file)
    user_file.load_dataframe()
    
    
if __name__ == '__main__':
    main()