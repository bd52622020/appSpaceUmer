'''
Created on 15 Jun 2020

@author: umer
'''
dictt={"Umer": "16-02-1995", "Sarmad": "11-10-1998", "Adam": "15-06-1988"}
print(dictt)
data = input(("Enter the name for birthday: "))
for key,value in dictt.items():
    if data == key:
        print(value)