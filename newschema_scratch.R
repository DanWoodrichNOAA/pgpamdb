library(pgpamdb)
library(DBI)

#pull template
source("./etc/paths.R")
con=pamdbConnect("schema_testing",keyscript,clientkey,clientcert)


template = dbFetch(dbSendQuery(con,"SELECT * FROM soundfiles"))
template$id = as.integer(template$id)
template$data_collection_id = as.integer(template$data_collection_id)

big_dataset = template[rep(1:nrow(template),100000),]
big_dataset$id = NULL
big_dataset$name = paste(1:nrow(big_dataset),"test",sep="")
big_dataset$duration=600

#50000 rows upload to sfs (no existing data)
#time:
#1st upload: 17.77849s
#2nd upload: 15.5076s

#now trying 500000
#time: 170s (roughly linear growth!)

start = Sys.time()
dbAppendTable(con,'soundfiles',big_dataset)
end = Sys.time()

#delete (19.59057s with 50000 rows:
#1 minute with 100000 rows !
#5.393357 minutes with 500000 rows (roughly linear as well)
start = Sys.time()
dbSendStatement(con,'DELETE FROM soundfiles')
end = Sys.time()

sfsall = dbFetch(dbSendQuery(con,"SELECT * FROM soundfiles"))
sfsall$id = as.integer(sfsall$id)

#time for updating 100 rows: 2.936336s
sfs_mod = sfsall[1:100,]
sfs_mod$duration = 235
start = Sys.time()
table_update(con,'soundfiles',sfs_mod)
end = Sys.time()
#1000 rows:  1.932044 s
sfs_mod = sfsall[100:1100,]
sfs_mod$duration = 235
start = Sys.time()
table_update(con,'soundfiles',sfs_mod)
end = Sys.time()
#1000 rows (growing): 1.93354 secs
sfs_mod = sfsall[1100:2100,]
sfs_mod$duration = 2
start = Sys.time()
table_update(con,'soundfiles',sfs_mod)
end = Sys.time()

test = dbFetch(dbSendQuery(con,"SELECT * FROM bins WHERE id IN (SELECT id FROM bins WHERE seg_end = 92)"))
test = dbFetch(dbSendQuery(con,"SELECT max(duration) FROM soundfiles"))


test1 = dbFetch(dbSendQuery(con,"SELECT * FROM soundfiles"))

#individually dl and upload tables into new schema.

hard_tables = c('soundfiles','bins','detections','bins_detections','bins_effort','effort_procedures')

hard_tables_todo = c('soundfiles','bins','detections','bins_detections','bins_effort','effort_procedures')

#soundfiles: get rid of 0 length sfs.
SFS = dbFetch(dbSendQuery(con_v2,paste("SELECT * FROM soundfiles")))
SFS=SFS[-which(SFS$duration==0),]
dbAppendTable(con_v3,'soundfiles',SFS)

bins = dbFetch(dbSendQuery(con_v2,paste("SELECT * FROM bins")))
bins=bins[which(bins$seg_end!=0),]
dbAppendTable(con_v3,'bins',bins)

#use these bin ids to remove matches to bins_detections, maybe bins_effort (hopefully not)?
bins_to_ignore_later =bins[which(bins$seg_end==0),]

bins_effort = dbFetch(dbSendQuery(con_v2,paste("SELECT * FROM bins_effort")))
dbAppendTable(con_v3,'bins_effort',bins_effort)

effort_procedures = dbFetch(dbSendQuery(con_v2,paste("SELECT * FROM effort_procedures")))
dbAppendTable(con_v3,'effort_procedures',effort_procedures)

#now just detections and bins detections.

dets = dbFetch(dbSendQuery(con_v2,paste("SELECT * FROM detections")))
zeroEnd_dets = dets[which(dets$end_time==0),] #look up a few of these in bins
#dets to see if they need to be recalculated. Wouldn't be surprised.
dets_to_ignore_later = zeroEnd_dets[which(zeroEnd_dets$start_time==0),]
zeroEnd_dets_no0s = zeroEnd_dets[-which(as.integer(zeroEnd_dets$id) %in% as.integer(dets_to_ignore_later$id)),]

#great. Now, subtract the zeroenddetsno0s from dets, rbind them, submit together.

dets = dets[-which(as.integer(dets$id) %in% as.integer(zeroEnd_dets$id)),]
dets = rbind(dets,zeroEnd_dets_no0s)

dbAppendTable(con_v3,"detections",dets)

#next: query and submit bins detections. remove all of the zero end dets
#and bins to ignore from the set.

bd = dbFetch(dbSendQuery(con_v2,paste("SELECT * FROM bins_detections")))
bdnew = bd[-which((as.integer(bd$bins_id) %in% as.integer(bins_to_ignore_later$id))|(
                 as.integer(bd$detections_id) %in% as.integer(zeroEnd_dets$id))),]

dbAppendTable(con_v3,'bins_detections',bdnew)

#rejecting the push: trying to change temporarily
#delete insert permission assigned to db developer
#change owner back to root.
#reverted both permissions, neither solved the issue

#attempt to save bdnew. system memory can't handle it.

write.csv(bdnew,'bdnew.csv')

#alteratively, I am going to attempt just resubmitting detections with
#the triggers in place. steps:

#copy whole detections to mem
dets = dbFetch(dbSendQuery(con_v3,paste("SELECT * FROM detections")))
#delete whole detections
dbSendStatement(con_v3,"DELETE * FROM detections")
#reenable all triggers
#submit detections.



#test- the only detections which should be missing from bd are those that
#I reassigned the end file and end time.

#also, check from bins to see if all are there.

#finally: turn triggers back on and
#delete the zero end dets no0s
#resubmit them.

#then, do some tests to see whether the db is still working!

#

start = Sys.time()
dbAppendTable(con_v3,'detections',dets)
end = Sys.time()
write.csv(paste(end-start,"s"),"runtime.csv")



con_v2=pamdbConnect("poc_v2",keyscript,clientkey,clientcert)
con_v3=pamdbConnect("poc_v3",keyscript,clientkey,clientcert)

tables = dbFetch(dbSendQuery(con_v3,"select * from information_schema.tables"))
tables_ = tables[which(tables$table_schema=="public"),"table_name"]

out = foreach(i=tables_) %do% {

  cnt_v2 = dbFetch(dbSendQuery(con_v2,paste("SELECT COUNT(*) FROM",i)))$count
  cnt_v3 = dbFetch(dbSendQuery(con_v3,paste("SELECT COUNT(*) FROM",i)))$count

  return(cbind(i,cnt_v2,cnt_v3))

}

out_tab = do.call('rbind',out)

