'''
Created on 2 Jun 2020

@author: umer
'''
def up_low(s):      
    u = sum(1 for i in s if i.isupper())
    l = sum(1 for i in s if i.islower())
    print( 'No. of Upper case characters : ',u)
    print('No. of Lower case characters :', l)

print('enter a string\n')
data = input()
up_low(data)