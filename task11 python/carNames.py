'''
Created on 15 Jun 2020

@author: umer
'''
dictt={"Sedan": 1500, "SUV": 2000, "Pickup": 2500, "Minivan": 1600, "Van": 2400, "Semi": 13600, "Bicycle": 7, "Motorcycle": 110}
myList = []
for key ,value in dictt.items():
    if value < 5000:
        myList.append(key.upper())
print(myList)
