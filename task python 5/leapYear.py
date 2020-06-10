'''
Created on 10 Jun 2020

@author: umer
'''
year = int(input("Please Enter the Year Number you wish: "))

if (( year%400 == 0) | (( year%4 == 0 ) & ( year%100 != 0))):
    print(year, "is a Leap Year")
else:
    print(year,"is Not the Leap Year")