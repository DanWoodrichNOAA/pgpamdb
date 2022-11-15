library(DbuddyTools)
library(RPostgres)
library(foreach)
library(tuneR)

#setwd("C:/Users/daniel.woodrich/Desktop/pgpamdb")

source("./R/functions.R") #package under construction

source("./etc/paths.R") #populates connection paths which contain connection variables.

con=pamdbConnect("poc",keyscript,clientkey,clientcert)

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


lookup_from_match <- function(conn,tablename,vector,match_col,idname='id'){

  id_and_name = paste(idname,match_col,sep=",")

  if(length(vector)>1){
    sf_id_lookup = paste("SELECT ",id_and_name," FROM ",tablename," WHERE ",match_col," IN",paste("(",paste("$",1:(length(vector)-1),collapse=",",sep=""),",$",length(vector),")",sep=""))
  }else{
    sf_id_lookup = paste("SELECT ",id_and_name," FROM ",tablename," WHERE ",match_col," IN",paste("($",length(vector),")",sep=""))
  }

  query1 <- dbSendQuery(conn, sf_id_lookup)
  dbBind(query1, params=vector)

  #dbCommit(conn)

  res = dbFetch(query1)

  dbClearResult(query1)

  res[[idname]]=as.integer(res[[idname]])

  return(res)

}




load_detx_detections<-function(conn,dataset){

  if(any(dataset$Type=='i_neg') | (!"Type" %in% colnames(dataset))){
    #mandate type be included to prevent accidental upload of i_neg data
    stop("protocol for i_neg not yet supported")
  }

  if((!"procedure" %in% colnames(dataset)) | (!"strength" %in% colnames(dataset))){

    stop("missing procedure or strength column")

  }

  if("LastAnalyst" %in% colnames(dataset) & (!"analyst" %in% colnames(dataset))){

    dataset$analyst = dataset$LastAnalyst
    dataset$LastAnalyst=NULL
  }

  #ids for soundfiles:
  sf_ids = lookup_from_match(con,"soundfiles",unique(c(dataset$StartFile,dataset$EndFile)),"name")
  dataset$StartFile = sf_ids$id[match(dataset$StartFile,sf_ids$name)]
  dataset$EndFile = sf_ids$id[match(dataset$EndFile,sf_ids$name)]

  #ids for label
  lab_id = lookup_from_match(con,"label_codes",dataset$label,"alias")
  dataset$label = lab_id$id[match(dataset$label,lab_id$alias)]

  #ids for signal_code
  sigcode_id = lookup_from_match(con,"signals",dataset$SignalCode,"code")
  dataset$SignalCode = sigcode_id$id[match(dataset$SignalCode,sigcode_id$code)]

  #ids for signal_code
  strength_id = lookup_from_match(con,"strength_codes",dataset$strength,"name")
  dataset$strength = strength_id$id[match(dataset$strength,strength_id$name)]

  if("analyst" %in% colnames(dataset)){

    #ids for lastanalyst
    pers_id = id_from_match(con,"personnel",dataset$analyst,"code")
    dataset$analyst = pers_id$id[match(dataset$analyst,pers_id$code)]

  }


  dataset$VisibleHz=NULL
  dataset$LastAnalyst=NULL
  dataset$Type= NULL
  dataset$id = NULL

  colnames(dataset)[match(c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","Comments","probs","SignalCode"),
                          colnames(dataset))]=c("start_time","end_time","low_freq","high_freq","start_file","end_file","comments","probability","signal_code")

  dataset$comments[which(is.na(dataset$comments))]=""

  print(str(dataset))

  print(paste("Submitting to database...",Sys.time()))

  rs = dbAppendTable(conn,"detections" , dataset)

}


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

load_detx_detections(conn,test2)

yes_so_far = dbFetch(dbSendQuery(con,"SELECT * FROM detections WHERE label = 1;"))

#cool, now let's take a peak at bins detections and see how things compare.

yes_so_far_bins = dbFetch(dbSendQuery(con,"SELECT * FROM bins WHERE lm = 1;"))

#compare the soundfiles in each!

dets_sf = unique(c(yes_so_far$start_file,yes_so_far$end_file))
bins_sf = unique(yes_so_far_bins$soundfiles_id)

#same soundfiles, at least! Looking at a few examples, all looks good.

