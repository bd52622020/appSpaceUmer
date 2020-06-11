'''
Created on 11 Jun 2020

@author: umer
'''
print("Input three integers(sides of a triangle)")
int_num = list(map(int,input().split()))
a,b,c = sorted(int_num)
if a**2+b**2==c**2:
    print('Yes')
else:
    print('No')