'''
Created on 9 Jun 2020

@author: umer
'''


data =[]
try:
    with open('commands.txt', 'r') as f:
        data.append(f.readlines(10))
except:
    print('file does not exit')
finally:
    print(data)

