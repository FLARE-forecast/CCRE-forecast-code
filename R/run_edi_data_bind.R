# source('R/edi_qaqc_combine.R')
# library(tidyverse)
# library(lubridate)
# library(yaml)
# library(EDIutils)
# 
# # open config file
# config_file <- read_yaml('~/CCRE-forecast-code/configuration/default/observation_processing.yml')
# 
# ## read offset file 
# entity_names <- read_data_entity_names(packageId = 'edi.1069.1')
# offset <- read_data_entity(packageId = 'edi.1069.1', entityId = entity_names$entityId[2])
# offset <- readr::read_csv(file = offset)
# 
# L1_file <- read.csv('https://raw.githubusercontent.com/addelany/CCRE-data/ccre-dam-data/ccre-waterquality_L1.csv', 
#                     na.strings = 'NA', stringsAsFactors = FALSE)
# 
# test_df <- wq_realtime_edi_combine(realtime_file = 'https://raw.githubusercontent.com/addelany/CCRE-data/ccre-dam-data/ccre-waterquality_L1.csv', 
#                             qaqc_file = 'https://portal.edirepository.org/nis/dataviewer?packageid=edi.1069.1&entityid=57267535da5ab0687d2fee52083699f8',
#                             offset_file = 'https://portal.edirepository.org/nis/dataviewer?packageid=edi.1069.1&entityid=b391093432e38eee7c7cc34ae977d553', 
#                             config_file = 'https://raw.githubusercontent.com/FLARE-forecast/CCRE-forecast-code/main/configuration/default/observation_processing.yml')
