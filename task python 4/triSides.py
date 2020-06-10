'''
Created on 9 Jun 2020

@author: umer
'''
def calculateSide(a,b):
    max_length = a + b - 1;  
    min_length = max(a, b) - min(a, b) + 1;  
  
    # Not a valid triangle  
    if (min_length > max_length) : 
        print(-1, end = "");  
        return;  
  
    print("Max length of side C=", max_length);  
    print("Min length of side C5=", min_length);

print('enter side a: ')
side1 = input()
print('enter side b:')
side2 = input()
calculateSide(int(side1), int(side2))