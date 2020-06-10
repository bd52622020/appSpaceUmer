'''
Created on 2 Jun 2020

@author: umer
'''
def pascal_triangle(n):
    trow = [1]
    y = [0]
    for x in range(0,n):
        print(trow)
        trow=[l+r for l,r in zip(trow+y, y+trow)]
    return n>=1
pascal_triangle(4)