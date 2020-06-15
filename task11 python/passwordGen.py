'''
Created on 15 Jun 2020

@author: umer
'''
import random
def getPassword(l):
    password = ''
    for c in range(l):
        password += random.choice(chars)
    print(password)
    
if __name__ =="__main__":
    print('''Password Generator''')
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@£$%^&*().,?0123456789'
    
    length = input('password length?')
    length = int(length)
    print('\n here are your passwords:') 
    getPassword(length)   





