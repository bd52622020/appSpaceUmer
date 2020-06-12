'''
Created on 12 Jun 2020

@author: umer
'''
x = [0,0,1,2,3,4,4,5,6,6,6,7,8,9,4,4]

old_value = None
new_lst = []

for data in x:
    if data != old_value:
        new_lst.append(data)
        old_value = data
        

print(new_lst)