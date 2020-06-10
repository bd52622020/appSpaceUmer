'''
Created on 8 Jun 2020

@author: umer
'''
rows = int(input("Enter the number of rows "))
for row in range(1, rows+1):
    for column in range(1, row + 1):
        print('*', end=' ')
    print("")

for i in range(rows-1, 0, -1):
    for j in range(1, i + 1):
        print('*', end=' ')
    print('')