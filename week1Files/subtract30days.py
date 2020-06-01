'''
Created on 1 Jun 2020

@author: umer
'''
from datetime import date, timedelta
dt = date.today() - timedelta(30)
print('Current Date :',date.today())
print('30 days before Current Date :',dt)