'''
Created on 8 Jun 2020

@author: umer
'''
class IOString():
    def __init__(self):
        self.str1 = ""

    def get_String(self):
        print('Enter a string')
        self.str1 = input()

    def print_String(self):
        print(self.str1.upper())

str1 = IOString()
str1.get_String()
str1.print_String() 