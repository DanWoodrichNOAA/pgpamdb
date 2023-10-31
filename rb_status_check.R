#cut down on keystrokes
dbGet <-function(x){
  x = gsub("[\r\n]", "", x)
  dbFetch(dbSendQuery(con,x))
}

library(pgpamdb)
library(DBI)

#pull template
source("./etc/paths.R")
con=pamdbConnect("poc_v3",keyscript,clientkey,clientcert)

#design a query to get the weekly % presence bins from the db.
#should roughly match: \\nmfs\akc-nmml\CAEP\Acoustics\ArcMAP\Mapping with R\Input files\Data frame\maDataRibbon.csv
template_ds = read.csv("//nmfs/akc-nmml/CAEP/Acoustics/ArcMAP/Mapping with R/Input files/Data frame/maDataRibbon.csv")

#the query will have to:
#join to soundfiles to get absolute timings of bins / scratch that- who cares if just locating to weeks. so much easier to use sfs.
#aggregate bins by week
#sum both total of bins with yes dets, and total of bins without.
#exclude weeks not represented in current effort

query = "SELECT 'RB',location_code,latidude,longitude,date_trunc('year', soundfiles.datetime)::date as Year,
date_trunc('week', soundfiles.datetime)::date AS Week, Week AS Date, As PercBinsWCalls FROM detections JOIN bins_detections ON detections.id = bins_detections.id
JOIN bins ON bins.id = bins_detections.id JOIN soundfiles ON bins.id = soundfiles.id JOIN
bins_effort ON bins.id = effort.bins_id JOIN effort ON effort.id = bins_effort.effort_id

GROUP BY location_code,week

"
