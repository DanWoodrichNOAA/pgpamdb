dbuddy_fg_convert_and_upload <-function(conn,fgname){

  #check that FG of name does not already exist

  num_matches = dbFetch(dbSendQuery(conn,paste("SELECT COUNT(*) FROM effort WHERE name = '",fgname,"'",sep="")))

  if(num_matches!=0){
    stop(paste("FG of name",fgname,"appears to already have been loaded to pgpamdb"))
  }

  test_fg=data_pull(paste("SELECT bins.*,soundfiles.DateTime,soundfiles.deployments_name FROM bins JOIN bins_filegroups ON bins.id = bins_filegroups.bins_id JOIN filegroups ON bins_filegroups.FG_name = filegroups.Name JOIN soundfiles ON bins.FileName= soundfiles.Name WHERE filegroups.Name = '",fgname,"';",sep=""))

  format_fg_query = data.frame(as.POSIXct(test_fg$DateTime,tz='utc'),test_fg$deployments_name,test_fg$SegStart,test_fg$SegStart+test_fg$SegDur)
  #so to associate to db, need to lookup by date, mooring id,

  colnames(format_fg_query) = c("soundfiles.datetime","data_collection.name","bins.seg_start","bins.seg_end")
  #Here is a template to do this sort of thing:

  #round to 2 decimals, like on db.

  format_fg_query$bins.seg_start = round(format_fg_query$bins.seg_start,2)
  format_fg_query$bins.seg_end = round(format_fg_query$bins.seg_end,2)

  out = table_dataset_lookup(conn,"SELECT bins.*,soundfiles.datetime,data_collection.name,a,b,c,d FROM bins JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id"
                              ,format_fg_query,c("timestamp","character varying","DOUBLE PRECISION","DOUBLE PRECISION"),return_anything = TRUE)

  #test if any of the bins don't exist
  if(nrow(out)<nrow(format_fg_query)){
    #means that at least some of the bins didn't exist already.

    #determine which didn't match.
    remaining = format_fg_query[which(!paste(format_fg_query$soundfiles.datetime,format_fg_query$data_collection.name,
                                            format_fg_query$bins.seg_start,format_fg_query$bins.seg_end) %in%
                                      paste(out[,"a"],out[,"b"],out[,"c"],out[,"d"])),]

    #of the remaining, submit as bins
    #lookup soundfile id

    sf_info =data.frame(remaining$soundfiles.datetime,remaining$data_collection.name)
    colnames(sf_info)=c("soundfiles.datetime","data_collection.name")

    sf_id_out = table_dataset_lookup(conn,"SELECT soundfiles.id,a,b FROM soundfiles JOIN data_collection ON data_collection.id = soundfiles.data_collection_id",
                               sf_info,c("timestamp","character varying"))

    remaining$soundfiles.id = sf_id_out[match(paste(remaining$soundfiles.datetime,remaining$data_collection.name),paste(sf_id_out$a,sf_id_out$b)),"id"]

    bins_ds = data.frame(remaining$soundfiles.id,remaining$bins.seg_start,remaining$bins.seg_end,as.integer(0))

    colnames(bins_ds) = c("soundfiles_id","seg_start","seg_end","type")

    bins_ds$soundfiles_id=as.integer(bins_ds$soundfiles_id)
    bins_ds$seg_start = as.numeric(bins_ds$seg_start)

    #submit bins

    dbAppendTable(conn,'bins',bins_ds)

    #now try again, to get ids:

    out = table_dataset_lookup(conn,"SELECT bins.*,soundfiles.datetime,data_collection.name,a,b,c,d FROM bins JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id"
                               ,format_fg_query,c("timestamp","character varying","DOUBLE PRECISION","DOUBLE PRECISION"))

  }

  #at this point, should have ids in out variable whether queried or submitted.

  fg_ids = as.integer(out$id)

  #upload FG

  oldFG = data_pull(paste("SELECT * FROM filegroups WHERE filegroups.Name = '",fgname,"';",sep=""))

  colnames(oldFG) =c("name","sampling_method","description")

  dbAppendTable(conn,'effort',oldFG)

  #get back id

  new_effort_id = dbFetch(dbSendQuery(conn,paste("SELECT id FROM effort WHERE name='",fgname,"'",sep="")))

  fg_tab = data.frame(fg_ids,as.integer(new_effort_id$id))
  colnames(fg_tab)=c("bins_id","effort_id")

  outrows = dbAppendTable(conn,"bins_effort",fg_tab)

  return(outrows)
}


library(DbuddyTools)
library(RPostgres)
library(foreach)
library(tuneR)

#setwd("C:/Apps/pgpamdb")

source("./R/functions.R") #package under construction

source("./etc/paths.R") #populates connection paths which contain connection variables.

con=pamdbConnect("poc_v2",keyscript,clientkey,clientcert)


#need to first add the signals to the signals table.

signals = dbFetch(dbSendQuery(con,"SELECT * FROM signals"))
signals_ = dbFetch(dbSendQuery(con,"SELECT * FROM signals_signal_origin"))

signals_mod = signals[which(signals$id==25),]

newtab = list()
all_signals = c(10,12,13,14,2,20,3,4)
for(i in 1:length(all_signals)){
  newtab[[i]]=signals_mod
}

newtab = do.call('rbind',newtab)

newtab$id = NULL

newtab$code = paste("HB.s.p.",all_signals,sep="")
newtab$description ="pattern _ of humpback song from AK/PIFSC from years 2015 - 2016."

#clear out incorrect naming conv from db.

dbFetch(dbSendQuery(con,"DELETE FROM signals_signal_origin WHERE signals_id = 25")) #hb is 5 btw in signal_origin
dbFetch(dbSendQuery(con,"DELETE FROM signals WHERE id = 25"))


newtab = data.frame(25:(24+nrow(newtab)),newtab)
colnames(newtab)[1]="id"
#done.

dbAppendTable(con,'signals',newtab)

#now add to signals_signal_origin:

newtab_ = data.frame(newtab$id,5,1,26:(25+8))
colnames(newtab_) = colnames(signals_)

dbAppendTable(con,'signals_signal_origin',newtab_)

#alright, looks like codes are properly registered now. Ready for me to load in detections + FGs!.
#already did XP14_UK_KO01_sample1
fgs_to_load = c("round1_pull1_reduce","XP15_UK_KO01_sample1","round1_pull2")

for(i in fgs_to_load){

  dbuddy_fg_convert_and_upload(con,i) #might want to split this function up into parts, so that I can also
  #add fg from the correctly formatted dataset. todo
}

#All look like they worked. After uploading more fg, might want to do a loop which at least checks number of rows..

#now for detections!

dets = data_pull("SELECT * FROM detections WHERE SignalCode IN ('HB.s.p.2','HB.s.p.3','HB.s.p.4','HB.s.p.10','HB.s.p.12','HB.s.p.13','HB.s.p.14','HB.s.p.20');")

#borrow from gt_fg_data_loader (should probably combine these two ultimately, or promote the fxns gt_fg and get rid of it)
cdets = dbuddy_pgpamdb_det_rough_convert(con,dets,procedure = 11,strength =2)

#ok, why not throw them all in there.
dbAppendTable(con,'detections',cdets)

#they went through- check out next time to see if they are behaving

#a more organized approach, may be to just discriminate the detections from dbuddy we know we don't want, and then
#load them all in. Do FG one by one after, since those are easier to distinguish. Otherwise, we might lose track of
#what's on there already.
