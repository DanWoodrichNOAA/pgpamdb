#in this script, try to write a few plotting functions to help visualize db results in a zoomed out way.
#heatmap by month and latitude divisions might be a good one!

#so to start it off, plotting by 'yes' bins vs 'no' bins + 'uk' bins throughout the data!

#query pulls sum of the above (in type 1)

library(RPostgres)
library(foreach)
library(tuneR)
library(ggplot2)


#setwd("C:/Apps/pgpamdb")

source("./R/functions.R") #package under construction

source("./etc/paths.R") #populates connection paths which contain connection variables.

#as I use this script, I'm just deleting and changing code to create the lookup table. to upload fgs only, I only use the first
#part of the loop below

con=pamdbConnect("poc_v2",keyscript,clientkey,clientcert)

#cut down on keystrokes
dbGet <-function(x){
  x = gsub("[\r\n]", "", x)
  dbFetch(dbSendQuery(con,x))
}

#this is what it will look like with gt. Can add layer by layer as I go throgh.rbmap
rbmap= bin_label_explore(con,'rb')
#rbmap_wgt = rbmap +add_layer_ble(con,23,10,"yellow")

#plot(rbmap_wgt)

fw_map = bin_label_explore(con,'fw')

fw_map_wgt =fw_map + add_layer_ble(con,5,10,"orange")
fw_map_wgt = fw_map_wgt + add_layer_ble(con,6,10,"orange")

plot(fw_map_wgt)

#little odd- not a very good distribution vs what is there! recall, only 7 comparison moorings were even available.
#Need to add the step where i superimpose another tile, or other indicator, on tiles which are not 0, which will help
#me identify all potential training sources.

#couple areas to common sense spot check- looks like there was training from a month where no fw was detected- check it out
#also, looks like there was a y in CH01- an accident? make a query and go see it.

#training is correct- no postives in training.
#High latitude positives- there is a mixed bag here. some obviously incorrect (WT01), some possibly incorrect (likely ag),
#some definitely correct. After I have tools to edit labels of deployment detections, correct obvious errors and send
#the rest around.
