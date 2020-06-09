'''
Created on 9 Jun 2020

@author: umer
'''
def point(team ='unknown',games =0, wins =0, loss =0, draw = 0):
    Points = 0
    if wins ==0:
        wins = games -loss - draw
        Points = Points + (wins * 3)
    elif loss ==0:
        loss = games -wins - draw
        Points = Points + 0
    elif draw ==0:
        wins = games -loss - wins
        Points = Points + draw
    else:
        Points = Points + (wins * 3)
        Points = Points + 0
        Points = Points + draw
    if  Points > 20:
        print('Points:', Points )
        print('good ! keep it up')
    elif Points >15 & Points <20:
        print('Points:', Points )
        print('they are gonna lose')
    else:
        print('Points:', Points )
        print('what a bunch of pathetic players')

point('Man UTD', 30, 10, 6, 5)