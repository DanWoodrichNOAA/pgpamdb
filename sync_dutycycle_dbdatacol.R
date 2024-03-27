dbGet <-function(x){
  x = gsub("[\r\n]", "", x)
  dbFetch(dbSendQuery(con,x))
}
#here is the original script, reference to preserve rounding rules etc.

#modify columns for database.
library(foreach)
library(tuneR)
library(dplyr)

source("./R/functions.R") #package under construction

source("./etc/paths.R") #populates connection paths which contain connection variables.

#as I use this script, I'm just deleting and changing code to create the lookup table. to upload fgs only, I only use the first
#part of the loop below

con=pamdbConnect("poc_v3",keyscript,clientkey,clientcert)

#this script will load in a csv of duty cycle, scan for change on db data, and update any changes made to
#duty cycle.

#what's tricky about this is that neither can really be considered a true golden copy. So, this
#utility should fill in gaps only in cases where information is absent, not when information is
#different.

#load in duty cycle:
duty_cycle = read.csv("C:/Users/daniel.woodrich/Downloads/newDutyCycle.csv")

duty_cycle$water_depth_m = as.numeric(duty_cycle$water_depth_m)
duty_cycle$startdata_datetime_utc = as.POSIXct(duty_cycle$startdata_datetime_utc,format="%m/%d/%Y %H:%M")
duty_cycle$enddata_datetime_utc = as.POSIXct(duty_cycle$enddata_datetime_utc,format="%m/%d/%Y %H:%M")

#load in data_collection:
data_col= dbGet("SELECT * FROM data_collection")

data_col$sampling_rate = as.integer(data_col$sampling_rate)

#strip down duty_cycle to only columns used in d_c
duty_cycle_format = data.frame(duty_cycle$mooring_deploy_name,duty_cycle$mooring_site_name,
                               duty_cycle$latitude_degN,duty_cycle$longitude_degW,
                               duty_cycle$water_depth_m,duty_cycle$sensor_depth_m,
                               duty_cycle$record_time_min,duty_cycle$cycle_time_min,
                               duty_cycle$startdata_datetime_utc,duty_cycle$enddata_datetime_utc,
                               duty_cycle$sampling_rate_hz)

colnames(duty_cycle_format) = c('name','location_code','latitude','longitude','water_depth','sensor_depth','rec_time','cycle_time','start_data','end_data','sampling_rate')

alldata = rows_patch(data_col, duty_cycle_format[which(duty_cycle_format$name %in% data_col$name),], by = "name")
alldata$id = as.integer(alldata$id)

#update the whole table:
table_update(con,'data_collection',alldata)
