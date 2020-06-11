'''
Created on 11 Jun 2020

@author: umer
'''
import platform as pl

os_profile = [
        'architecture',
        'linux_distribution',
        'mac_ver',
        'machine',
        'node',
        'platform',
        'processor',
        'python_build',
        'python_compiler',
        'python_version',
        'release',
        'system',
        'uname',
        'version',
    ]
for key in os_profile:
    if hasattr(pl, key):
        print(key +  ": " + str(getattr(pl, key)()))
        
