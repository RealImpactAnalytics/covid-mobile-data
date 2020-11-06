#!/usr/bin/env python
# coding: utf-8

# In[1]:


import datetime as dt
from covid_mobile_data.cdr_aggregation.notebooks.modules.DataSource import *
from covid_mobile_data.cdr_aggregation.notebooks.modules.folder_utils import *


# In[2]:


#Set relative file path to config file
config_file = '../config_file.py'
exec(open(config_file).read())


# In[3]:


#Create the DataSource object and show config
ds = DataSource(datasource_configs)
ds.show_config()


# In[4]:


#Setup all required data folders
setup_folder(ds)


# In[5]:


#Check if required data folders already exists
check_folders(ds)


# In[ ]:




