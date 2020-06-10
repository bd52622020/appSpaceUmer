'''
Created on 10 Jun 2020

@author: umer
'''
print('enter the range of prime numbers')
data = int(input())
for i in range(1,data):
    if i > 1:
        for j in range(2,i):  
            if (i % j) == 0:  
                break  
        else:  
            print(i)