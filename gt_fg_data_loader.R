dbuddy_pgpamdb_det_rough_convert<-function(conn,dbuddy_data,procedure,strength){

  #determine these values through matches
  label_lookup= lookup_from_match(conn,'label_codes',unique(dbuddy_data$label),'alias')
  labels = label_lookup$id[match(dbuddy_data$label,label_lookup$alias)]
  sc_lookup= lookup_from_match(conn,'signals',unique(dbuddy_data$SignalCode),'code')
  signal_codes = sc_lookup$id[match(dbuddy_data$SignalCode,sc_lookup$code)]
  la_lookup = lookup_from_match(conn,'personnel',unique(dbuddy_data$LastAnalyst),'code')
  last_analyst = la_lookup$id[match(dbuddy_data$LastAnalyst,la_lookup$code)]

  outdata =data.frame(dbuddy_data[,c(2:8,11)],procedure,labels,signal_codes,strength,last_analyst)

  colnames(outdata)=c("start_time","end_time","low_freq","high_freq","start_file","end_file","probability","comments","procedure","label","signal_code","strength","analyst")

  file_lookup = lookup_from_match(conn,"soundfiles",unique(c(outdata$start_file,outdata$end_file)),"name")
  outdata$start_file = file_lookup$id[match(outdata$start_file,file_lookup$name)]
  outdata$end_file = file_lookup$id[match(outdata$end_file,file_lookup$name)]

  outdata[which(is.na(outdata$comments)),"comments"]=""

  return(outdata)

}

i_neg_interpolate<-function(data,FG,high_freq,procedure,signal_code,analyst){

  FG = FG[,which(colnames(FG) %in% c("soundfiles_id","seg_start","seg_end","datetime"))]

  FG = FG[order(FG$datetime),]

  FG$delete = 0

  #combine rows on condition
  for(i in 2:nrow(FG)){
    if(FG[i-1,"soundfiles_id"]==FG[i,"soundfiles_id"] & FG[i-1,"seg_end"]==FG[i,"seg_start"]){
      FG[i-1,"delete"]=1
      FG[i,"seg_start"]=FG[i-1,"seg_start"]
    }
  }

  FG = FG[which(FG$delete==0),]

  #FG now represents largest consectutive section not overlapping soundfiles.

  #make FG

  #for each bin loop extract dets from fg and then determine which detections to add
  new_dets = list()
  for(i in 1:nrow(FG)){
    #need to design this so it can handle detections which have start/end times relative to another file.
    #probably going to be a pain in the ass. do tomorrow.

    dets= data[which((data$start_file == FG$soundfiles_id[i] & data$start_file>= FG$seg_start[i]) | (data$end_file == FG$soundfiles_id[i] & data$end_file<= FG$seg_end[i]) ),]

    times = data.frame(c(dets$start_time,dets$end_time),c(dets$start_file,dets$end_file),c(rep("start",nrow(dets)),rep("end",nrow(dets))))
    colnames(times)=c("time","file","meaning")

    times = times[which(times$file==FG$soundfiles_id[i]),]

    times$time= as.numeric(times$time)

    #if the earliest time is a 'start', add an 'end' time to start of FG
    if(nrow(times)>0){
      if(times[which.min(times$time),"meaning"]=='start'){
        times = rbind(c(FG$seg_start[i],as.integer(FG$soundfiles_id[i]),'end'),times)
      }
    }else{
      times = rbind(c(FG$seg_start[i],as.integer(FG$soundfiles_id[i]),'end'),times)
    }

    colnames(times)=c("time","file","meaning")

    if(times[which.max(times$time),"meaning"]=='end'){
      times = rbind(c(FG$seg_end[i],as.integer(FG$soundfiles_id[i]),'start'),times)
    }

    times$time= as.numeric(times$time)

    #now, go row to row in dets. every time hit a start, cap the detection. retain the earliest time through each iteration.

    times = times[order(times$time),]

    start_time=0
    counter = 1
    detsout = list()
    for(p in 1:nrow(times)){

      if(times$meaning[p]=='end'){
        start_time = max(start_time,times$time[p]) #this will chose the latest possible start time in case of multiple ends overlapping.
      }else{
        detsout[[counter]]=c(start_time,times$time[p])
        counter = counter + 1
      }

    }

    detsout = do.call("rbind",detsout)

    #take the above and turn into detections. assume some same fields as source data.

    detsout = data.frame(as.numeric(detsout[,1]),as.numeric(detsout[,2]),0,high_freq,as.integer(FG$soundfiles_id[i]),
                         as.integer(FG$soundfiles_id[i]),NA,"",procedure,0,signal_code,2,analyst)

    new_dets[[i]] = detsout

  }

  new_dets=do.call('rbind',new_dets)

  colnames(new_dets) = c("start_time","end_time","low_freq","high_freq","start_file","end_file","probability","comments","procedure","label","signal_code","strength","analyst")

  return(new_dets)
}


#GT and FG data loader.

library(DbuddyTools)
library(RPostgres)
library(foreach)
library(tuneR)

#setwd("C:/Apps/pgpamdb")

source("./R/functions.R") #package under construction

source("./etc/paths.R") #populates connection paths which contain connection variables.

con=pamdbConnect("poc_v2",keyscript,clientkey,clientcert)

#previously- here's how I got a FG and translated it into db form.

#extract FG info from bins
test_fg=data_pull("SELECT bins.*,soundfiles.DateTime,soundfiles.deployments_name FROM bins JOIN bins_filegroups ON bins.id = bins_filegroups.bins_id JOIN filegroups ON bins_filegroups.FG_name = filegroups.Name JOIN soundfiles ON bins.FileName= soundfiles.Name WHERE filegroups.Name = 'BS15_AU_PM02-a_files_1-104_rw_hg';")

format_fg_query = data.frame(as.POSIXct(test_fg$DateTime,tz='utc'),test_fg$deployments_name,test_fg$SegStart,test_fg$SegStart+test_fg$SegDur)
#so to associate to db, need to lookup by date, mooring id,

colnames(format_fg_query) = c("soundfiles.datetime","data_collection.name","bins.seg_start","bins.seg_end")
#Here is a template to do this sort of thing:

out = table_dataset_lookup(con,"SELECT bins.*,soundfiles.datetime,data_collection.name,a,b,c,d FROM bins JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id"
                           ,format_fg_query,c("timestamp","character varying","DOUBLE PRECISION","DOUBLE PRECISION"))

#looks like it worked. so, just associate ids
fg_ids = as.integer(out$id)
fg_tab = data.frame(fg_ids,1)
colnames(fg_tab)=c("bins_id","effort_id")
dbAppendTable(con,"bins_effort",fg_tab)

#now for this FG, try to get ids.

tempout = paste(getwd(),"test.csv.gz",sep="/")
system(paste("dbuddy pull detections",tempout,"--FileGroup BS15_AU_PM02-a_files_1-104_rw_hg.csv --Analysis_ID 10 --SignalCode RW --Type i_neg --label y"))
temp =read.csv(tempout)
file.remove(tempout)

#function to convert from dbuddy to pgpamdb
out2 = dbuddy_pgpamdb_det_rough_convert(con,temp,procedure = 10,strength=2)

#now, need a function to interpolate negatives
out3 = i_neg_interpolate(out2,out,1024,10,"RW","DFW")

#combine with positive data:

out_all = rbind(out2,out3)

#upload i_neg data
dbAppendTable(con,"detections",out_all) #currently only BS15_AU_PM02-a_files_1-104_rw_hg is in!
