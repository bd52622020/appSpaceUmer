'''
Created on 12 Jun 2020

@author: umer
'''
from urllib.request import urlopen
from bs4 import BeautifulSoup
html = urlopen('https://en.wikipedia.org/wiki/Main_Page')
bs = BeautifulSoup(html, "html.parser")
headerData = bs.find_all(['h1', 'h2','h3','h4','h5','h6'])
print('List all the header tags :', *headerData, sep='\n\n')