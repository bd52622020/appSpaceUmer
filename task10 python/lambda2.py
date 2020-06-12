'''
Created on 12 Jun 2020

@author: umer
'''
data = [19, 65, 57, 39, 152, 639, 121, 44, 90, 190]
print("Orginal list:")
print(data) 
result = list(filter(lambda x: (x % 19 == 0 or x % 13 == 0), data)) 
print("\nNumbers of the above list divisible by 19 or 13:")
print(result)