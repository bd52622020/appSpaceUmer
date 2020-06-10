'''
Created on 10 Jun 2020

@author: umer
'''
import re

pattern = '^[0-9]{1}[-0-9]{4}[-0-9]{6}[-0-9]{2}$'
code = input("Please Enter ISBN code: ")
if re.match(pattern,code):
    print(code,' is correct')
else:
    print(code, 'is wrong')