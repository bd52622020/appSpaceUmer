import os
print('hello world')
f = open('Shakespeare.txt','rt')
data = f.read()
words = data.split()
print('number of words =',len(words))
######counting lines
lineCount = 0
for line in data:
    lineCount +=1
print('number of lines =', lineCount)
#####vowel count
vowel = ('AEIOUaeiou')
vCount = 0
for v in data:
    if v in vowel:
        vCount +=1
print('number of vowels =', vCount)
####count digits
numbers = sum(c.isdigit() for c in data)
print('numbers of digits =',numbers)
import csv

with open('resutls.csv', 'w') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
    filewriter.writerow(['number of words', int(len(words))])
    filewriter.writerow(['number of lines', int(lineCount)])
    filewriter.writerow(['number of vowels',int(vCount)])
    filewriter.writerow(['number of digits', int(numbers)])
