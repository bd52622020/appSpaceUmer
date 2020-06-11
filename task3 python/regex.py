'''
Created on 3 Jun 2020

@author: umer
'''
import re 
 
pattern= "^[A-Za-z\s]" 

txt = "We love programming with Big Data" 

if re.search(pattern,txt):
    print('match found')
else:
    print('not found')