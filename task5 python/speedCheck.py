'''
Created on 9 Jun 2020

@author: umer
'''
def speedCheck(speed):
    if speed == 70:
        print('ok')
    elif speed >70:
        speed1  = (speed-70)//5
        if speed1<12:
            print('points:', speed1)
        else:
            print('license Suspended')

data = input('enter speed: ')
data = int(data)
speedCheck(data)