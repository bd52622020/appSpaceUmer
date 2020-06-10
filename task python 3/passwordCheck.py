'''
Created on 8 Jun 2020

@author: umer
'''
import re

pattern = '(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[@$#])'
print('enter a password between 6-16 character')
strr = input()
if len(strr)<=6:
    print('password too short')
elif len(strr)>=16:
    print('password too long')
if re.match(pattern, strr):
    print(strr)
else:
    print('mush contain atleasr 1 upper case 1 lower case 1 digit and one character for [@#$]')