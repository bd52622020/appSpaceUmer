'''
Created on 8 Jun 2020

@author: umer
'''
name_dict ={}
n =10 
i =1
while i <= 5:
    print('enter name \n')
    name_dict[i] = input()
    i = i+1
def convert_to_list(data):
    mylist =[]
    for name in name_dict.values():
        mylist.append(name)
    mylist = sorted(mylist)
    print(mylist)
convert_to_list(name_dict)

    