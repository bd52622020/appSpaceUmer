#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkConf, SparkContext  


# In[2]:


#function to get movie list
def loadMovieNames():
    movieNames = {}
    with open("u.item",encoding='utf-8') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames

#functions for get desired data
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))

def occupation(lines):
    fields = lines.split('|')
    return (int(fields[0]), (str(fields[3])))

def usrRating(data):
    fields = data.split()
    return (int(fields[0]),int(fields[1]),(int(fields[2]), 1.0))

def ratingDate(data):
    fields = data.split()
    return (int(fields[1]),int(fields[3]))

def ratingMovie(data):
    fields = data.split()
    return (int(fields[1]),int(fields[2]))

def movieRelease(data):
    fields = data.split('|')
    return (int(fields[0]),str(fields[2]))

def usrAge(data):
    fields = data.split('|')
    return (int(fields[0]), int(fields[1]))


# In[3]:


# The main script - create our SparkContext
conf = SparkConf().setAppName("WorstMovies")
sc = SparkContext(conf = conf)


# In[4]:


# Load up the raw u.data file
lines = sc.textFile("u.data")
# Convert to (movieID, (rating, 1.0))
movieRatings = lines.map(parseInput)
#get occupation
user = sc.textFile("u.user")
occuData = user.map(occupation)
#get userid mid and rating
usrMvRt = lines.map(usrRating)
#getting mvID and mvNames
movieNames = loadMovieNames()


# In[5]:


#Q top 10 rated movies sorted by rating
# Reduce to (movieID, (sumOfRatings, totalRatings))
ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )
# Map to (movieID, averageRating)
mvRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[1])
# Sort by average rating
sortedMovies = mvRatings.sortBy(lambda x: x[1],False)
# Take the top 10 results
results = sortedMovies.take(10)
# top 10 rated movies
for result in results:
    print(movieNames[result[0]], result[1])


# In[6]:


#Most Rated Movie by Students
#sorted by user id
sortedusrMvRt = usrMvRt.sortBy(lambda x: x[0])
#only getting user that are stuent
student = occuData.filter(lambda x:x[1] == 'student')
student.take(10)
#converting rdd into list
student2 = student.collect()
#function to get onlu students for u.data
def getStu(x):
    for stu in student2:
        if x == stu[0]:
            return x

#all movies rated by students
stuRating = sortedusrMvRt.filter(lambda x:x[0] == getStu(x[0]))
#funciton to retrieve only movie id rated by student from 
def temp(data):
    return (data[1],data[2])
#the rdd
temprdd=stuRating.map(temp)
#counting the rating
ratingCount = temprdd.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )
# Map to (movieID, Rating)
stRatings = ratingCount.mapValues(lambda totalAndCount : totalAndCount[1])
# Sort by rating
sortedStRt = stRatings.sortBy(lambda x: x[1],False)
# Take the top result
resultSt = sortedStRt.take(1)
# top rated movie by student
for result in resultSt:
    print(movieNames[result[0]], result[1])
#temprdd.take(5)


# In[7]:


#Q movies that were rated after 1960
from datetime import datetime
dateRating = lines.map(ratingDate)
#function to get dates from unixtimestamp
def getDate(x):
    temp = datetime.utcfromtimestamp(x[1]).strftime('%Y')
    return(x[0],int(temp))

properDates = dateRating.map(getDate)
ratedAfter1960 = properDates.filter(lambda x:x[1]>1960).take(20) 
for result in ratedAfter1960:
    if result[1] == 1960:
        print('ERROR')
    else:
        print(movieNames[result[0]], result[1])


# In[8]:


#Most Rated movie by 20-25
usrLife = user.map(usrAge)
setAge20 = usrLife.filter(lambda x:x[1] >= 20)
setAge = setAge20.filter(lambda x:x[1] <= 25)
setAge2 = setAge.collect()
def getAge(x):
    for ag in setAge2:
        if x == ag[0]:
            return x

#all movies rated by 20-25
ageRating = sortedusrMvRt.filter(lambda x:x[0] == getAge(x[0]))
ageRating.take(10)
def temp2(data):
    return (data[1],data[2])
#the rdd
temprdd2=ageRating.map(temp2)
#counting the rating
ageCount = temprdd2.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )
# Map to (movieID, Rating)
agRatings = ageCount.mapValues(lambda totalAndCount : totalAndCount[1])
# Sort by rating
sortedAgRt = agRatings.sortBy(lambda x: x[1],False)
# Take the top result
resultAg = sortedAgRt.take(1)
# top rated movie by student
for result in resultAg:
    print(movieNames[result[0]], result[1])
#temprdd.take(5)


# In[9]:


#top movies rated 5 
mvRated5 = movieRatings.filter(lambda x:x[1][0] == 5.0)
mvRated5Count = mvRated5.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )
# Map to (movieID, Rating)
mvRated5Ratings = mvRated5Count.mapValues(lambda totalAndCount : totalAndCount[1])
# Sort by rating
sortedmvRated5 = mvRated5Ratings.sortBy(lambda x: x[1],False)
# Take the top result
resultmvRated5 = sortedmvRated5.take(10)
# top rated movie by student
for result in resultmvRated5:
    print(movieNames[result[0]], result[1])
#temprdd.take(5)


# In[10]:


#top ten zipcode with most rated movies
def usrZipcode(data):
    fields = data.split('|')
    return (fields[4], (int(fields[0]),1.0))

def usrZipcode2(data):
    fields = data.split('|')
    return (int(fields[0]), fields[4])

usrZip = user.map(usrZipcode)
usrZipCount = usrZip.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )
# getting of most zipcode by user
usrZipRatings = usrZipCount.mapValues(lambda totalAndCount : totalAndCount[1])
# getting top 10 zipcode by the count of user
sortedusrZip = usrZipRatings.sortBy(lambda x: x[1],False).take(10)

def getZip(x):
    for zi in sortedusrZip:
        if x == zi[0]:
            return x
#getting user of top ten zipcode 
usrZip2 = user.map(usrZipcode2)
usrZip3 = usrZip2.filter(lambda x:x[1]==getZip(x[1]))
usrZip4 = usrZip3.collect()

def getZipT(x):
    for zi in usrZip4:
        if x == zi[0]:
            return x

#all movies rated by users of top ten zipcodes
mstZipUsr = sortedusrMvRt.filter(lambda x: x[0]==getZipT(x[0]))


# In[11]:


#top ten zipcode with most rated movies (Count.D)
def temp3(data):
    return (data[1],data[2])
#getting movies and ratings only
temprdd3=mstZipUsr.map(temp3)
mstZipCount = temprdd3.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )
# Map to (movieID, Rating)
mstZipRatings = mstZipCount.mapValues(lambda totalAndCount : totalAndCount[1])
# Sort by rating
sortedmstZip = mstZipRatings.sortBy(lambda x: x[1],False)
# Take the top result
resultmstZip = sortedmstZip.take(10)
# top rated movie by student
for result in resultmstZip:
    print(movieNames[result[0]], result[1])


# In[12]:


#Q oldest movie rated 5
ratMv = lines.map(ratingMovie)
#movies only rated 5
ratMv2 = ratMv.filter(lambda x:x[1]==5)
release = sc.textFile('u.item')
relMv = release.map(movieRelease)
#function to get dates from string
def getFormat(x):
    temp = datetime.strptime(x[1],'%d-%b-%Y')
    temp2 = datetime.date(temp)
    return(x[0],temp2)

tempDates = relMv.filter(lambda x:x[1 != ''])
formatDates = tempDates.map(getFormat)
oldest = formatDates.sortBy(lambda x :x[1]).take(1)
ratMv3 = ratMv2.filter(lambda x:x[0]==oldest[0][0]).take(1)
for result in ratMv3:
    print(movieNames[result[0]], result[1])


# In[23]:


#Q Genres of top rated movies
def getGenre(data2):
    x = data2.split('|')
    return(int(x[0]),           int(x[5]),           int(x[6]),           int(x[7]),           int(x[8]),           int(x[9]),           int(x[10]),           int(x[11]),           int(x[12]),           int(x[13]),           int(x[14]),           int(x[15]),           int(x[16]),           int(x[17]),           int(x[18]),           int(x[19]),           int(x[20]),           int(x[21]),           int(x[22]),           int(x[23]))

movie = sc.textFile('u.item')
mvGenre = movie.map(getGenre)
mvGenre.take(10)


# In[39]:


#Q Genres of top rated movies (Count.D)
a = {0:'unknown',     1:'Action',     2:'Adventure',     3:'Animation',     4:'Childrens',     5:'Comedy',     6:'Crime',     7:'Documentry',     8:'Drama',     9:'Fantasy',     10:'Film_noir',     11:'Horror',     12:'Musical',     13:'Mystery',     14:'Romance',     15:'Sci_fi',     16:'Thriller',     17:'War',     18:'Western',}

lst2 = []
def getGrenes2(y):
    mid  = y[0]
    lst2 = []
    strm = ''
    key = 0
    value =''
    if y[1] == 1:
        for key, value in a.items():
            if value =='unkown':
                lst2.append((mid,key))
    else:
        pass
    if y[2] == 1:
        for key, value in a.items():
            if value=='Action':
                lst2.append((mid,key))
    else:
        pass
    if y[3] == 1:
        for key, value in a.items():
            if value =='Adventure':
                lst2.append((mid,key))
    else:
        pass
    if y[4] == 1:
        for key, value in a.items():
            if value =='Animation':
                lst2.append((mid,key))
    else:
        pass
    if y[5] == 1:
        for key, value in a.items():
            if value =='Childrens':
                lst2.append((mid,key))
    else:
        pass
    if y[6] == 1:
        for key, value in a.items():
            if value =='Comedy':
                lst2.append((mid,key))
    else:
        pass
    if y[7] == 1:
        for key, value in a.items():
            if value =='Crime':
                lst2.append((mid,key))
    else:
        pass
    if y[8] == 1:
        for key, value in a.items():
            if value =='Documentry':
                lst2.append((mid,key))
    else:
        pass
    if y[9] == 1:
        for key, value in a.items():
            if value =='Drama':
                lst2.append((mid,key))
    else:
        pass
    if y[10] == 1:
        for key, value in a.items():
            if value =='Fantasy':
                lst2.append((mid,key))
    else:
        pass
    if y[11] == 1:
        for key, value in a.items():
            if value =='Film_noir':
                lst2.append((mid,key))
    else:
        pass
    if y[12] == 1:
        for key, value in a.items():
            if value =='Horror':
                lst2.append((mid,key))
    else:
        pass
    if y[13] == 1:
        for key, value in a.items():
            if value =='Musical':
                lst2.append((mid,key))
    else:
        pass
    if y[14] == 1:
        for key, value in a.items():
            if value =='Mystery':
                lst2.append((mid,key))
    else:
        pass
    if y[15] == 1:
        for key, value in a.items():
            if value =='Romance':
                lst2.append((mid,key))
    else:
        pass
    if y[16] == 1:
        for key, value in a.items():
            if value =='Sci_Fi':
                lst2.append((mid,key))
    else:
        pass
    if y[17] == 1:
        for key, value in a.items():
            if value =='Thriller':
                lst2.append((mid,key))
    else:
        pass
    if y[18] == 1:
        for key, value in a.items():
            if value =='War':
                lst2.append((mid,key))
    else:
        pass
    if y[19] == 1:
        for key, value in a.items():
            if value =='Western':
                lst2.append((mid,key))
    else:
        pass 
    
    #for l in lst2:
        #return(l[0],l[1])
    return(lst2)
    
            
def getTopGenre(x):
    for res in results:
        if x[0] == res[0]:
            return(x[0],x[1],res[1])

movieWithGenre = mvGenre.map(getGrenes2)
movieWithGenre2 = movieWithGenre.flatMap(lambda x: x)
genreOfTopTen = movieWithGenre2.map(getTopGenre)
tempGen = genreOfTopTen.filter(lambda x:x != None).collect()
for d in tempGen:
    print(movieNames[d[0]],a[d[1]],d[2])


# In[72]:


#Q top 10 Genres by Rating
tempGenList = movieWithGenre2.collect()
def getGenre4(x):
    for temp in tempGenList:
        if x[0] == temp[0]:
            return (temp[1],x[1])
        
movieRating2 = movieRatings.sortBy(lambda x:x[0])
tempGenRating = movieRating2.map(getGenre4)
tempGenRating2 = tempGenRating.filter(lambda x:x != None)
tempGenRating2.take(10)
tempGenCount = tempGenRating2.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )
tempGenRatings = tempGenCount.mapValues(lambda totalAndCount : totalAndCount[1])
# Sort by rating
sortedtempGen = tempGenRatings.sortBy(lambda x: x[1],False)
# Take the top result
resulttempGen = sortedtempGen.take(10)
# top rated movie by student
for result in resulttempGen:
    print(a[result[0]], result[1])


# In[ ]:




