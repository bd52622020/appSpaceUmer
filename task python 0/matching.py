'''
Created on 1 Jun 2020

@author: umer
'''
import re
def data_match(text):
        pattern = '^[a-zA-Z\d_\s]*$'
        if re.search(pattern,  text):
            print('Found a match!')
        else:
            print('Not matched!')

data_match("The quick brown fox jumps over the lazy dog.")
data_match("Correct data_01")
