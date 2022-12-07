library(DbuddyTools)
library(RPostgres)
library(foreach)
library(tuneR)

#setwd("C:/Apps/pgpamdb")

source("./R/functions.R") #package under construction

source("./etc/paths.R") #populates connection paths which contain connection variables.

con=pamdbConnect("poc_v2",keyscript,clientkey,clientcert)

 #look at deployments currently loaded, ponder if loading it all up to postgres is a good idea:

data_collection_previous = data_pull("SELECT * FROM deployments;")

data_collection_to_load = data.frame(data_collection_previous$Name,
                                     data_collection_previous$MooringID,
                                     data_collection_previous$Latitude,
                                     data_collection_previous$Longitude,
                                     data_collection_previous$WaterDepth,
                                     data_collection_previous$SensorDepth,
                                     data_collection_previous$RecTime,
                                     data_collection_previous$CycleTime,
                                     #data_collection_previous$StartData,
                                     #data_collection_previous$EndData,
                                     data_collection_previous$SamplingRate,
                                     data_collection_previous$WavByHand)

colnames(data_collection_to_load)=c("name",
                                    "location_code",
                                    "latitude",
                                    "longitude",
                                    "water_depth",
                                    "sensor_depth",
                                    "rec_time",
                                    "cycle_time",
                                   # "start_data",
                                    #"end_data",
                                    "sampling_rate",
                                    "wav_by_hand")

data_collection_to_load$sensor_depth= as.numeric(data_collection_to_load$sensor_depth)

data_collection_to_load$id = NULL
#data_collection_to_load$start_data don't worry about this yet- looks a little funked up and don't really need it as all the relevant date info is in soundfiles.

#looks like it worked, for some reason? But, I want to try again with original ids... so

#dbSendQuery(con,"TRUNCATE data_collection RESTART IDENTITY CASCADE;")

dbAppendTable(con,"data_collection" , data_collection_to_load)

dc_return = dbFetch(dbSendQuery(con,"SELECT * FROM data_collection;"))

#just to try some things out, see if I can update the data using ids to populate historic name.
old_names_lookup = read.csv("C:/Users/daniel.woodrich/Desktop/database/dbuddy_tools/data_internal/mooring_name_lookup_edit.csv")

dc_return$historic_name = old_names_lookup$Old.B.drive.name[match(dc_return$name,old_names_lookup$New.mooring.name)]

#command = paste("UPDATE data_
replace <- dbSendQuery(con,command)
dbBind(replace, params=data)

#super gross to have to do this one at a time, but I am getting issues using the ? syntax which should take data.

#in general, R may not be a good choice for bulk inserts. I should look up python?
#below is a strategy using glue, which I should probably explore.
#https://community.rstudio.com/t/postgres-parameterized-insert-update-optimization/17380/8
#this is just awful- only useful for one offs, and extremely questionable even then since it should be a single transaction.

for(i in 1:nrow(dc_return)){
  update <- dbSendQuery(con, 'UPDATE data_collection SET historic_name = $1 WHERE id = $2')
  dbBind(update, params=c(dc_return$historic_name[i],as.integer(dc_return$id[i])))
}

#revisiting this: going to try again, but this time see if I can make it a single transaction:
#first, remove the added data.
dbSendQuery(con, "UPDATE data_collection SET historic_name = 'test'")

#now try as one transaction:
dbBegin(con)

for(i in 1:nrow(dc_return)){
  print(i)
  #update = dbExecute(con, paste("UPDATE data_collection SET historic_name = $1 WHERE id = ",as.integer(dc_return$id[i]),sep=""))
  update <- dbSendQuery(con, 'UPDATE data_collection SET historic_name = $1 WHERE id = $2')
  dbBind(update, params=c(dc_return$historic_name[i],as.integer(dc_return$id[i])))
}

dbCommit(con)

#the above still sends each component query to the db inidividually, instead of sending a single large query to the server. Attempt to assemble a large query and send.
#https://stackoverflow.com/questions/18936896/updating-multiple-rows-with-different-primary-key-in-one-query-in-postgresql
query = "update data_collection as m
set historic_name = c.column_a
from (values
      ($1::integer, $4),
      ($2::integer, $5),
      ($3::integer, $6)
) as c(id, column_a)
where c.id = m.id"

query <- gsub("[\r\n]", "", query)

update <- dbSendQuery(con, query)
dbBind(update, params=c(as.character(dc_return$id[1:3]),dc_return$historic_name[1:3]))

#it worked! Now just need to extend to arbitrary datasets, and if this is the best solution, add some built in type casting.

#parameters: 1. table name 2. vector of column names to modify 3. dataset of equal length of id + cols in 2

#command =dbSendQuery(con,"UPDATE data_collection SET :historic_name WHERE :id")

testq = "SELECT data_type,column_name
               FROM information_schema.columns
               WHERE table_schema = 'public'
                     AND table_name ='data_collection'
                     AND column_name IN ('historic_name','latitude')"

testq <- gsub("[\r\n]", "", testq)

out = dbFetch(dbSendQuery(con,testq))

#tablename = "data_collection"
#colvector = c("historic_name")
dataset = data.frame(dc_return$id,dc_return$historic_name)
colnames(dataset)= c("id","historic_name")


#sweet! it works. Save this function into new package, and use as my update function. Unsure if I will hit any character restrictions, but shouldn't be updating
#huge quantities of data as normal practice...?
table_update(con,"data_collection",dataset)

#update <-dbSendQuery(con,"UPDATE data_collection as t SET t.historic_name = historic_name FROM (VALUES %(id,historic_name)) WHERE t.id=id")

#UPDATE contact as t SET
#name = data.name
#FROM (VALUES %s) AS data (id, name)
#WHERE t.id = data.id

#rootpath = "//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves"
#mooring_name = "AL18_AU_UN01-b"
#conn = con

load_soundfile_metadata(con,"//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves","AL18_AU_UN01-b")

dbDisconnect(con)

#try to load in some LM result data into the db.

query="SELECT DISTINCT detections.* FROM filegroups
JOIN bins_filegroups ON filegroups.Name = bins_filegroups.FG_name
JOIN bins ON bins.id = bins_filegroups.bins_id
JOIN detections ON bins.FileName = detections.StartFile
WHERE filegroups.Name = 'AL18_AU_UN01-b' AND detections.SignalCode = 'LM' AND detections.Type = 'DET';"

query <- gsub("[\r\n]", "", query)

test=data_pull(query)

#that worked, the only problem is that we already knew that it was on here which is boring. Kind of want to upload another mooring. AW15_AU_BS2?

print(dbFetch(dbSendQuery(con,"SELECT name FROM data_collection WHERE historic_name = 'AW15_AU_BS2'")))

#and by that I mean AW15_AU_BS02. Ok, upload this mooring so I can get some good LM data up.
#running this on 164

load_soundfile_metadata(con,"//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves","AW15_AU_BS02")

#look to see what description in analysis was for id 5.

old_a_id= unique(test$Analysis_ID)

data_collection_previous = data_pull(paste("SELECT * FROM analyses WHERE id = ",old_a_id,";",sep=""))

#In the meantime, load up the LM data which I already have. First step is to correctly format it. This should probably be a function so I can repeat it. For now just try it
#in the script body.

#actually, I kind of think one big 'superfunction' to translate from INSTINCT standards to db standards would be good.
#The advantage here is that it can look up integer keys with queries based on the data provided. If i_neg data is provided, simply reject it (for now) until it
#is supported.

#try to query to get soundfile id from soundfile name.

#query2 = "SELECT id,name FROM soundfiles WHERE name IN ('AU-ALUN01_b-181005-193000.wav','AU-ALUN01_b-181008-013000.wav')"

#coincedentally, this is the same id from one database to the other. need to check the differences before loading.
#test$Analysis_ID = 5

test$procedure = test$Analysis_ID

test$Analysis_ID = NULL

test$strength = 'tight' #need to add manually.

test$procedure=5






#query2 <- gsub("[\r\n]", "", query2)

#out = dbFetch(dbSendQuery(con,query2))

#ok, that worked so just dbind above

m_name = "'AW15_AU_BS02'" #'AL18_AU_UN01-b'

query2=paste("SELECT DISTINCT detections.* FROM filegroups
JOIN bins_filegroups ON filegroups.Name = bins_filegroups.FG_name
JOIN bins ON bins.id = bins_filegroups.bins_id
JOIN detections ON bins.FileName = detections.StartFile
WHERE filegroups.Name = ",m_name," AND detections.SignalCode = 'LM' AND detections.Type = 'DET';")

query2 <- gsub("[\r\n]", "", query2)

test2=data_pull(query2)

test2$procedure = test$Analysis_ID

test2$Analysis_ID = NULL

test2$strength = 'tight' #need to add manually.

test2$procedure=5

#for low moan data, probably the best way to do it is to first load all the data as CTW, then modify the DFW reviewed data.
#however, tricky to persist the ids after loading them in (haven't been assigned yet) and query back the same data.
#We could assume that the original data that was reviewed was y lm by Cole, but probably not an ironclad enough assumption.
#probably best decision is to just load the data as it is.

#load_detx_detections(con,test2)

yes_so_far = dbFetch(dbSendQuery(con,"SELECT * FROM detections WHERE label = 1;"))

#cool, now let's take a peak at bins detections and see how things compare.

yes_so_far_bins = dbFetch(dbSendQuery(con,"SELECT * FROM bins WHERE lm = 1;"))

#compare the soundfiles in each!

dets_sf = unique(c(yes_so_far$start_file,yes_so_far$end_file))
bins_sf = unique(yes_so_far_bins$soundfiles_id)

#same soundfiles, at least! Looking at a few examples, all looks good.

#now, delete all the LM data currently on there.

dbLMs = dbFetch(dbSendQuery(con,"SELECT * FROM detections WHERE status = 1;"))

arch_data = dbLMs

arch_data$status = 2

table_update(con,"detections",arch_data,colvector=c("status"),idname = 'id')

#it went through- test if it worked.

dbLMs = dbFetch(dbSendQuery(con,"SELECT * FROM detections WHERE status = 1;"))
#empty!
dbLMs = dbFetch(dbSendQuery(con,"SELECT * FROM detections WHERE status = 2;"))
#here we go.

#based on rules, now there should be empty set if querying bins.

yes_so_far_bins = dbFetch(dbSendQuery(con,"SELECT * FROM bins WHERE lm = 1;"))

#correct!

#cool, that's good. Now, delete the rows. Try out R dbi fxn.
#looks like there is nothing built in to do it based on ids. So, create the function

out = table_delete(con,'detections',dbLMs$id,hard_delete = TRUE)

#cool, that worked.

#now, build an insert function.

#first, rebuild our LM set.

m_names = c("'AW15_AU_BS02'","'AL18_AU_UN01-b'") #'AL18_AU_UN01-b'

query2=paste("SELECT DISTINCT detections.* FROM filegroups
JOIN bins_filegroups ON filegroups.Name = bins_filegroups.FG_name
JOIN bins ON bins.id = bins_filegroups.bins_id
JOIN detections ON bins.FileName = detections.StartFile
WHERE filegroups.Name IN (",paste(m_names,collapse=","),") AND detections.SignalCode = 'LM' AND detections.Type = 'DET';",sep="")

query2 <- gsub("[\r\n]", " ", query2)

test3=data_pull(query2)

test3$procedure = test$Analysis_ID

test3$Analysis_ID = NULL

test3$strength = 'tight' #need to add manually.

test3$procedure=5


ids = table_insert(con,"detections",detx_to_db(test3))

dbdata = detx_to_db(con,test3)

data = data.frame(ids,dbdata)

data$comments=paste("test: id is ",data$id)

table_update(con,'detections',data)

datatest = data[,c("id","comments")]

datatest$comments = ""

table_update(con,'detections',datatest)

dbLMs = dbFetch(dbSendQuery(con,"SELECT * FROM detections WHERE status = 2;"))

#archived the above test, so delete archive with test comments
out = table_delete(con,'detections',dbLMs$id)

count_dbLMs = dbFetch(dbSendQuery(con,"SELECT COUNT(*) FROM detections WHERE status = 1;"))


#ok, now assemble the insert query. This time, return ids.

#this will update the sql dataset using an R dataset. requires column names of R dataset to be identical.

#try to upload first cole data (assume cole origin), then modify it and resubmit.

og_analyst = test3$LastAnalyst

test3$LastAnalyst="CTW"

dbdata = detx_to_db(con,test3)

ids = table_insert(con,"detections",dbdata)

test3$LastAnalyst=og_analyst

dbdata2 = detx_to_db(con,test3)

dbdata2 = data.frame(ids,dbdata2)

table_update(con,"detections",dbdata2[,c("id","analyst")])

#we can get the original set, by querying for the analysis/effort, and then in R- taking the "oldest 2 of every duplicated og_id"

dbLMs = dbFetch(dbSendQuery(con,"SELECT * FROM detections;"))

#maybe instead of below try to design the query in SQL.

#test_query = "SELECT id,start_time,end_time,low_freq,high_freq,start_file,
#end_file,probability,comments,procedure,label,signal_code,strength,
#MIN(modified),analyst,original_id FROM detections GROUP BY original_id
#"

test_query = "SELECT original_id,MIN(modified) FROM detections GROUP BY original_id"

test_query <- gsub("[\r\n]", " ", test_query)

#oldest_dets = dbFetch(dbSendQuery(con,test_query))

test_query = "SELECT * FROM detections t1
WHERE modified = ( SELECT MIN( t2.modified )
                FROM detections t2
                WHERE t1.original_id = t2.original_id )"

test_query <- gsub("[\r\n]", " ", test_query)

oldest_dets = dbFetch(dbSendQuery(con,test_query))

current_dets = dbFetch(dbSendQuery(con,"SELECT * FROM detections WHERE status = 1;"))


#test: do a pheaux SC upload.

#1st: query the bins from one of the moorings:

query = "SELECT seg_start,seg_end,soundfiles_id,sampling_rate FROM bins
JOIN soundfiles ON bins.soundfiles_id = soundfiles.id
JOIN data_collection ON soundfiles.data_collection_id = data_collection.id
WHERE data_collection.name = 'AL18_AU_UN01-b' AND bins.type=2" # LIMIT 100"

query <- gsub("[\r\n]", " ", query)

bins_tab = dbFetch(dbSendQuery(con,query))

template_tab = dbFetch(dbSendQuery(con,"SELECT * FROM detections LIMIT 1"))

template_tab = template_tab[0,which(!colnames(template_tab) %in% c("id","original_id","modified","analyst","status"))]

#now populate the table

bins_tab_full = data.frame(bins_tab[,c(1,2)],0,bins_tab[,4]/2,bins_tab[,3],bins_tab[,3],NA,'dummy data',0,sample(c(0,1),nrow(bins_tab),replace = TRUE),15,1)
colnames(bins_tab_full) = colnames(template_tab)

#first test: see how fast if don't need to return ids.

#start at 4:47, end at 6:05 for 1 hr 18 minutes total.
dbAppendTable(con,"detections" , bins_tab_full)

#ok, clear out this test
del_ids = dbFetch(dbSendQuery(con,"SELECT id FROM detections WHERE comments = 'dummy data'"))

#start at 8:30, end at ???
table_delete(con,'detections',del_ids$id,hard_delete = TRUE)

#test out querying bin table wide.
pres_bins_low = dbFetch(dbSendQuery(con,"SELECT * FROM bins WHERE lm = 1"))

#create small dataset to test upload and delete speed.

bins_tab_full_reduce = bins_tab_full[1:100,]
bins_tab_full_reduce = bins_tab_full[1:200,]
bins_tab_full_reduce = bins_tab_full[1:400,]
bins_tab_full_reduce = bins_tab_full[10001:29124,]

#some value in the test set from 20000 to 30000 is clogging up the works!

#time smaller dataset (100 rows of 225s bins with randomly generated labels)
#took 3.9624 seconds,4.25 seconds..
#200 rows: took 8.441 seconds
#400 rows: 16.63956 secs
#1000 rows
datainsert = data.frame(c(1000,5000,10000,20000,25000,30000,45000,58248),c( 1.13,4.87,9.607884,19.93,23.71,28.2186,42.52,55.4))


#testing while data are loaded to dets and bins
#1000 rows, 22 seconds (retain dets)
#9000 rows, 11 seconds (huh?)
#19k rows, 23.61 seconds (hmm...)


#test with 1000 rows with bin relabel disabled lasted 37.47s
#so- 37.47 of 41.923 secs of insert is dedicated to just det/bin calc!!
#meaning, relabel bin likely not the focus.

starttime = Sys.time()
dbAppendTable(con,"detections" , bins_tab_full_reduce)
enttime = Sys.time() - starttime

dbFetch(dbSendQuery(con,"SELECT count(*) FROM bins where dk = 1"))
dbFetch(dbSendQuery(con,"SELECT count(*) FROM bin_label_wide where dk != 99"))
dbFetch(dbSendQuery(con,"SELECT count(*) FROM detections"))
dbFetch(dbSendQuery(con,"SELECT count(*) FROM bins_detections"))
dbFetch(dbSendQuery(con,"SELECT count(*) FROM det_to_archive"))

#now time delete operation
#100 rows:
#took 4.191708 secs,4.39 secs..
#200 rows:
# took: 8.675018 seconds
#400 rows:
# 17.55512 seconds
#1000
# 45.85782
datadelete= data.frame(c(1000,5000,10000,58248),c(1.03,3.52,7.626942,13.37,39.37))
starttime = Sys.time()
dbSendQuery(con,"DELETE FROM detections")
enttime = Sys.time() - starttime

starttime = Sys.time()
dbFetch(dbSendQuery(con,"SELECT id FROM bins where bins.soundfiles_id = 10440"))
enttime = Sys.time() - starttime

#create an experiment where I load progressively more dummy data, and log duration of operations.

moorings = c("AL16_AU_CL01","AW15_AU_BS02","AL20_AU_PM02-b","AW12_AU_KZ01","AW14_AU_BF03",
             
             "XB17_AM_PR01","RW10_EA_BS02","CZ11_AU_IC03-04","BS11_AU_PM05","BF10_AU_BF03",
             "BF07_AU_BF05","AW15_AU_NM01","AW14_AU_BS02","AL21_AU_UM01","AL19_AU_NM01",
             "AL16_AU_BS03","AW15_AU_WT01","BF07_AU_BF04","XB17_AU_LB01","XB17_AM_OG01")

moorings = dir("//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves")

data_col = dbFetch(dbSendQuery(con,paste("SELECT name,sampling_rate from data_collection WHERE sampling_rate >0 and name IN ('",paste(moorings,collapse = "','",sep=""),"')",sep="")))

moorings=data_col$name

signal_tab =dbFetch(dbSendQuery(con,"SELECT * from signals"))

all_binned = signal_tab[which(!is.na(signal_tab$native_bin)),]

row_num = 1
mooring = c()
signal_code = c()
bintype = c()
time_per = c()
total_time = c()

moorings_done = dbFetch(dbSendQuery(con,"SELECT DISTINCT data_collection.name from soundfiles JOIN data_collection ON 
                                     data_collection.id = soundfiles.data_collection_id"))

moorings_to_go = moorings[-which(moorings %in% moorings_done$name)]

#just moorings
for(i in 1:length(moorings_to_go)){
  
  starttime = Sys.time()
  val = load_soundfile_metadata(con,"//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves",moorings_to_go[i])
  endtime = difftime(Sys.time(),starttime,units="secs")
  
  mooring = c(mooring,moorings_to_go[i])
  #signal_code = c(signal_code,97) #upload 'code' for now.
  bintype = c(bintype,97)
  time_per = c(time_per,endtime/val)
  total_time = c(total_time,endtime)
  
  write.csv(data.frame(mooring,bintype,time_per,total_time),"outlog11.csv")
  
}


for(i in 1:length(moorings)){

  starttime = Sys.time()
  val = load_soundfile_metadata(con,"//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves",moorings[i])
  endtime = difftime(Sys.time(),starttime,units="secs")

  mooring = c(mooring,moorings[i])
  #signal_code = c(signal_code,97) #upload 'code' for now.
  bintype = c(bintype,97)
  time_per = c(time_per,endtime/val)
  total_time = c(total_time,endtime)

  write.csv(data.frame(mooring,bintype,time_per,total_time),"outlog11.csv")
  
  
  #for every mooring, load up every bin

  for(n in 1:3){

    query = paste("SELECT seg_start,seg_end,soundfiles_id,sampling_rate FROM bins
    JOIN soundfiles ON bins.soundfiles_id = soundfiles.id
    JOIN data_collection ON soundfiles.data_collection_id = data_collection.id
    WHERE data_collection.name = '",moorings[i],"' AND bins.type=",n,"",sep="")

    query <- gsub("[\r\n]", " ", query)

    bins_tab = dbFetch(dbSendQuery(con,query))

    template_tab = dbFetch(dbSendQuery(con,"SELECT * FROM detections LIMIT 1"))

    template_tab = template_tab[0,which(!colnames(template_tab) %in% c("id","original_id","modified","analyst","status"))]

    #now populate the table
    
    data_full = NULL

    for(p in 1:sum(n==all_binned$native_bin)){

      row = all_binned[which(n==all_binned$native_bin)[p],]

      bins_tab_full = data.frame(bins_tab[,c(1,2)],0,bins_tab[,4]/2,bins_tab[,3],bins_tab[,3],NA,'dummy data',0,sample(c(0,1),nrow(bins_tab),replace = TRUE),as.integer(row$id),1)
      colnames(bins_tab_full) = colnames(template_tab)
      
      data_full = rbind(data_full,bins_tab_full)

      #starttime = Sys.time()
      #print(paste(n,as.integer(row$id),Sys.time()))
      #val = dbAppendTable(con,"detections" , bins_tab_full)
      #dbAppendTable(con,"detections" , bins_tab_full)
      #endtime = difftime(Sys.time(),starttime,units="secs")
      
      #time difference with labeling: 0.001536158 secs
      #time difference w/0 labeling: 0.0005130881 secs
      #alright, so it looks like labeling is taking more time!
      #after vacuum:  0.001557289 secs.. no effect :(
      
      #Time difference of 0.002097361 secs
      #after staging: Time difference of 0.002038302 secs

      #mooring = c(mooring,moorings[i])
      #signal_code = c(signal_code,as.integer(row$id)) #upload 'code' for now.
      #bintype = c(bintype,n)
      #time_per = c(time_per,endtime/val)
      #total_time = c(total_time,endtime)

      #write.csv(data.frame(mooring,signal_code,bintype,time_per,total_time),"outlog9.csv")
      
      #dbSendQuery(con,"ANALYZE detections")
      #dbSendQuery(con,"ANALYZE bins_detections")

    }
    
    
    starttime = Sys.time()
    print(paste(n,Sys.time()))
    val = dbAppendTable(con,"detections" , data_full)
    #dbAppendTable(con,"detections" , bins_tab_full)
    endtime = difftime(Sys.time(),starttime,units="secs")
    
    mooring = c(mooring,moorings[i])
    #signal_code = c(signal_code,as.integer(row$id)) #upload 'code' for now.
    bintype = c(bintype,n)
    time_per = c(time_per,endtime/val)
    total_time = c(total_time,endtime)
    
    write.csv(data.frame(mooring,bintype,time_per,total_time),"outlog10.csv")


  }


}


#interpret the data:

rundata = read.csv("outlog3.csv")
rundata = read.csv("outlog4.csv")
rundata = read.csv("outlog5.csv")
rundata = read.csv("outlog6.csv")
rundata = read.csv("outlog7.csv")
rundata = read.csv("outlog9.csv")

rundata$vals = as.integer(rundata$total_time/rundata$time_per)


rundata_nosf = rundata[which(rundata$bintype!=97),]

rundata_sf = rundata[which(rundata$bintype==97),]

plot(rundata_sf$X,rundata_sf$time_per)

plot(rundata_nosf$X,rundata_nosf$time_per,col=factor(rundata_nosf$bintype))






time_by_moor = aggregate(rundata$total_time,list(rundata$mooring),sum)

plot(factor(time_by_moor$Group.1),time_by_moor$x)


time_by_moor_nosf = aggregate(rundata_nosf$total_time,list(rundata_nosf$mooring),sum)



plot(factor(time_by_moor_nosf$Group.1),time_by_moor_nosf$x)



time_by_moor_nosf2 = aggregate(rundata_nosf$time_per,list(rundata_nosf$mooring),mean)

time_by_moor_nosf2$order = c(1,3,4,5,2)

plot(time_by_moor_nosf2$order,time_by_moor_nosf2$x)

fit2 <- lm(order~poly(x,2,raw=TRUE), data=time_by_moor_nosf2)

lines(1:5, fitted(fit2),col='red')

abline(a=min(time_by_moor_nosf2$x)-0.000075, b=0.000065,col='red')


time_by_moor_nosf$order = c(1,3,4,5,2)

plot(time_by_moor_nosf$order,time_by_moor_nosf$x)


#now try: delete the detections and soundfiles corresponding to first mooring (AL16_AU_CL01)

query ="SELECT detections.id FROM data_collection
JOIN soundfiles ON data_collection.id = soundfiles.data_collection_id JOIN
detections ON detections.start_file = soundfiles.id OR detections.end_file = soundfiles.id WHERE
data_collection.name = 'AL16_AU_CL01';"


query <- gsub("[\r\n]", " ", query)

ids_to_del = dbFetch(dbSendQuery(con,query))

ids_to_del$id<-as.integer(ids_to_del$id)

table_delete(con,"detections",ids_to_del$id,hard_delete = TRUE)

query ="SELECT soundfiles.id FROM data_collection
JOIN soundfiles ON data_collection.id = soundfiles.data_collection_id WHERE
data_collection.name = 'AL16_AU_CL01';"

query <- gsub("[\r\n]", " ", query)

ids_to_del = dbFetch(dbSendQuery(con,query))

ids_to_del$id<-as.integer(ids_to_del$id)

idz = ids_to_del$id[2:length(ids_to_del$id)]

starttime = Sys.time()
table_delete(con,"soundfiles",ids_to_del$id[201:300])
enttime = Sys.time() - starttime

#alright, see if a rnomal query will do it, or just if it is for delete? 

starttime = Sys.time()
dbFetch(dbSendQuery(con,"SELECT COUNT(*) FROM bins_detections WHERE bins_id IN (SELECT id FROM bins WHERE soundfiles_id = 101)"))
enttime = Sys.time() - starttime


#test with 1000 rows with bin relabel disabled lasted 36.99141s
#uhh, don't even have to recalc in delete- how does that make any sense?

#given the above, we should take a look at decisions for det/bin calc?

#so, insert takes 95% of time of deletion. See how that scales up with size. both seem to scale ok at this scale.

#alright, starting with 0 triggers, turning them on 1 by 1.
#no triggers solves the problem-
#1000 rows in .2s. delete similarly fast.
#turning on:
#comp_to_bins_ins: 37.14697 secs. There it is!

#so the problem with insert is comp_to_bins_ins. How about for delete?
#I enabled all of the triggers it seems to need (ins triggers still disabled.). But it went super fast???
#trying again- this time, upload with triggers, then delete with no insert triggers.
#ok, something is definitely up! can't figure out what is making delete take so long.

#try to disable just the archive detections... that did it!

#So what did we learn?
#The bottleneck is only in bin_recalc. Delete just also hits that bottleneck, since it triggers an insert.
#strategies: bring down bin_recalc. optionally, can disable recalc for status 2 detections, and instead just hard input the old
#old bins_detections values into bins_detections.In other words, only trigger recalc if necessary (positional columns change)

#but if we can sharply reduce bin calculation, shouldn't be a problem.

#seeing if I can optimize the above bottleneck query. One idea is to make a smaller table than bins, by removing species columns.

#get the critical columns in bins:

query = "SELECT id,soundfiles_id,seg_start,seg_end,type FROM bins"

query <- gsub("[\r\n]", " ", query)

bins_all = dbFetch(dbSendQuery(con,query))

dbWriteTable(con, "bins_test", bins_all)

#cool- using the smaller bins table saved ~8 seconds! That's big. So, we definitely at least want to divide up the tables.
#for that, we will need a trigger which populates the one-to-one table when bins are created, as well as keeps ids consistent between
#them.

#the second, probably quite fruitful thing to try is to instead of do a per row query do a

#try this query:

query = "SELECT detections.id,detections.start_file,detections.end_file,detections.start_time,detections.end_time,bins.id,bins.seg_start,bins.seg_end FROM detections
 JOIN bins ON detections.start_file = bins.soundfiles_id OR detections.end_file = bins.soundfiles_id"

query <- gsub("[\r\n]", " ", query)

test = dbFetch(dbSendQuery(con,query))

query <- gsub("[\r\n]", " ", "SELECT COUNT(*) FROM detections")
dbFetch(dbSendQuery(con,query))

query <- gsub("[\r\n]", " ", "SELECT id FROM detections LIMIT 10000")

ids_to_del = dbFetch(dbSendQuery(con,query))

#delete speed test: 9061512 starting detection rows. 

starttime = Sys.time()
#table_delete(con,"detections",ids_to_del$id,hard_delete = TRUE)
dbFetch(dbSendQuery(con,'DELETE FROM detections WHERE id < 2000000'))
dbFetch(dbSendQuery(con,'DELETE FROM detections WHERE original_id < 2000000'))
enttime = Sys.time() - starttime

#at 1000 (9061512 total dets): 14 rows per sec.
#at 10000 (9060512 total dets):  rows per sec. 


#test updating: 


query <- gsub("[\r\n]", " ", "SELECT id,label FROM detections WHERE signal_code = 1 LIMIT 100000")
ids_to_upd<-dbFetch(dbSendQuery(con,query))

ids_to_upd$label = 1

starttime = Sys.time()
table_update(con,'detections',ids_to_upd)
enttime = Sys.time() - starttime

query <- gsub("[\r\n]", " ", "SELECT *FROM detections WHERE id = 1244449")
sf_ids<-dbFetch(dbSendQuery(con,query))

query <- gsub("[\r\n]", " ", paste("SELECT * FROM bin_label_wide WHERE id IN (",paste(sf_ids$start_file,collapse=",",sep=""),") LIMIT 100"))
rslt<-dbFetch(dbSendQuery(con,query))

#seems to be scaling ok. Didn't get a chance to test out the above thoroughly- so, probably need to still do testing. But looking good from a performance perspective.

starttime1 = Sys.time()
dbSendQuery(con,"DELETE FROM detections")
enttime1 = Sys.time() - starttime1

starttime2 = Sys.time()
dbSendQuery(con,"DELETE FROM detections")
enttime2 = Sys.time() - starttime2



