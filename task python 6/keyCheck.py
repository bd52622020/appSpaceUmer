'''
Created on 11 Jun 2020

@author: umer
'''
dic = {1: 10, 2: 20, 3: 30, 4: 40, 5: 50, 6: 60}
def checkKey(x):
    if x in dic:
        print('Key is present in the dictionary')
    else:
        print('Key is not present in the dictionary')
checkKey(5)
checkKey(9)