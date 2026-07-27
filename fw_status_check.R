#LM status check

library(pgpamdb)
library(DBI)

#pull template
source("./etc/paths.R")
con=pamdbConnect("AFSC",keyscript,clientkey,clientcert)

table = dbFetch(dbSendQuery(con,"SELECT COUNT(*),data_collection.name,label FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE label != 20 AND status = 1 AND detections.procedure = 25 GROUP BY data_collection.name,label"))

#takes a million years to run, likely need to optimize now that there are 100s of m of rows in db
#out2 = procedure_prog(con,c(6,7,25))
#plot(out2[[1]])
#View(out2[[2]])

#see where we have run moorings. break into chunks due to memory overflow db.
moorings_run_25 = dbFetch(dbSendQuery(con,"SELECT COUNT(*),data_collection.name,label FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE label != 20 AND status = 1 AND detections.procedure = 25 GROUP BY data_collection.name,label,procedure"))
moorings_run_6 = dbFetch(dbSendQuery(con,"SELECT COUNT(*),data_collection.name,label FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE label != 20 AND status = 1 AND detections.procedure = 6 GROUP BY data_collection.name,label,procedure"))
moorings_run_7 = dbFetch(dbSendQuery(con,"SELECT COUNT(*),data_collection.name,label FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE label != 20 AND status = 1 AND detections.procedure = 7 GROUP BY data_collection.name,label,procedure"))

moorings_run_all = rbind(moorings_run_25,moorings_run_6)
#pull in all moorings
all_moorings = dbFetch(dbSendQuery(con,"SELECT name FROM data_collection WHERE institution_source='AFSC'"))
#determine what moorings need to be peak reviewed

#those with ONLY 99s:
peak_review_needed_moorings <- setdiff(moorings_run_all$name[moorings_run_all$label == 99], moorings_run_all$name[moorings_run_all$label != 99])

#determine what moorings need to be run:
moorings_todo  = all_moorings[-which(all_moorings$name %in% moorings_run_all$name),]

#first: run a subset of these on airflow. From there, can test against previous run process (local inf test on cloud)
#and make sure outputs are looking ok. Then, can fully scale out.

#BS24_AU_PM02
#SF16_AC_SD53: in progress, exclude
#GA23_AU_SU01
