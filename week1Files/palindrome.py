'''
Created on 1 Jun 2020

@author: umer
'''
print('enter a string: ')
data =input()
if str(data) == str(data)[::-1]:
    print('True')
else:
    print('False')