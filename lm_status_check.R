#LM status check

library(pgpamdb)
library(DBI)

#pull template
source("./etc/paths.R")
con=pamdbConnect("poc_v3",keyscript,clientkey,clientcert)

table = dbFetch(dbSendQuery(con,"SELECT COUNT(*),data_collection.name FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE detections.procedure = 23 AND detections.label = 99 GROUP BY data_collection.name"))

out = procedure_prog(con,c(5,23))
plot(out[[1]])
View(out[[2]])

out2 = procedure_prog(con,c(6,7,25))
plot(out2[[1]])
View(out2[[2]])

test = dbFetch(dbSendQuery(con,"SELECT * FROM data_collection"))
