#!/usr/bin/env python
# coding: utf-8

# In[8]:


#Q1
from math import radians, sin, cos, acos

print("Input coordinates of two points:")
slat = radians(float(input("Starting latitude: ")))
slon = radians(float(input("Ending longitude: ")))
elat = radians(float(input("Starting latitude: ")))
elon = radians(float(input("Ending longitude: ")))

dist = 6371.01 * acos(sin(slat)*sin(elat) + cos(slat)*cos(elat)*cos(slon - elon))
print("The distance is %.2fkm." % dist)


# In[5]:


#Q2
test_list = [2,4,5,6,8] 
print("The original list : " ,test_list) 

mean = sum(test_list) / len(test_list) 
variance = sum([((x - mean) ** 2) for x in test_list]) / len(test_list) 
res = variance ** 0.5
  
# Printing result 
print("Standard deviation is : " , res)


# In[14]:


import pandas as pd
import numpy as np
np.random.seed(24)
df = pd.DataFrame({'A': np.linspace(1, 10, 10)})
df = pd.concat([df, pd.DataFrame(np.random.randn(10, 3), columns=list('BCD'))],
               axis=1)
print("Original array:")
print(df)
def color_negative_red(val):
    if val < 0 :
        color = 'red'
    else:
        color = 'black'
    return 'color: %s' % color
print("\nNegative numbers red and positive numbers black:")
df.style.applymap(color_negative_red)


# In[ ]:




