dbuddy_pgpamdb_det_rough_convert<-function(conn,dbuddy_data,procedure,strength,depnames= NULL){


  #determine these values through matches
  label_lookup= lookup_from_match(conn,'label_codes',unique(dbuddy_data$label),'alias')
  labels = label_lookup$id[match(dbuddy_data$label,label_lookup$alias)]
  sc_lookup= lookup_from_match(conn,'signals',unique(dbuddy_data$SignalCode),'code')
  signal_codes = sc_lookup$id[match(dbuddy_data$SignalCode,sc_lookup$code)]
  la_lookup = lookup_from_match(conn,'personnel',unique(dbuddy_data$LastAnalyst),'code')
  last_analyst = la_lookup$id[match(dbuddy_data$LastAnalyst,la_lookup$code)]

  outdata =data.frame(dbuddy_data[,c(2:8,11)],procedure,labels,signal_codes,strength,last_analyst)

  colnames(outdata)=c("start_time","end_time","low_freq","high_freq","start_file","end_file","probability","comments","procedure","label","signal_code","strength","analyst")

  if(!is.null(depnames)){

    temp = data.frame(c(outdata$start_file,outdata$end_file),c(depnames,depnames))

    if(any(duplicated(temp))){

      temp = temp[-which(duplicated(temp)),]
    }

    colnames(temp) = c("soundfiles.name","data_collection.name")

    #lookup by table.
    file_lookup = table_dataset_lookup(conn,"SELECT soundfiles.id,soundfiles.name,data_collection.name,a,b FROM soundfiles JOIN data_collection ON soundfiles.data_collection_id = data_collection.id"
                                       ,temp,c("character varying","character varying"))

  }else{
    file_lookup = lookup_from_match(conn,"soundfiles",unqfiles,"name")
  }
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

    #bugged horribly. Fixed.
    dets= data[which((data$start_file == FG$soundfiles_id[i] & data$start_time>= FG$seg_start[i]) & (data$end_file == FG$soundfiles_id[i] & data$end_time<= FG$seg_end[i]) ),]

    times = data.frame(c(dets$start_time,dets$end_time),c(dets$start_file,dets$end_file),c(rep("start",nrow(dets)),rep("end",nrow(dets))))
    colnames(times)=c("time","file","meaning")

    times = times[which(times$file==FG$soundfiles_id[i]),]

    times$time= as.numeric(times$time)

    #View(times[order(times$time),])

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

    #print(detsout)

    if(any(duplicated(detsout))){

      stop()
    }
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

#as I use this script, I'm just deleting and changing code to create the lookup table. to upload fgs only, I only use the first
#part of the loop below

con=pamdbConnect("poc_v2",keyscript,clientkey,clientcert)

fgs_dbuddy =data_pull("SELECT name FROM filegroups;")
fgs_dbuddy_hg = fgs_dbuddy[which(grepl("LMyesSample_1",fgs_dbuddy$Name)),]
#add the species id to id- assume and review
fgs_dbuddy_hg2= substr(fgs_dbuddy_hg,nchar(fgs_dbuddy_hg)-4,nchar(fgs_dbuddy_hg)-3)

lookup = data.frame(fgs_dbuddy_hg,fgs_dbuddy_hg2)
#subtract the data which is already loaded:
lookup=lookup[-which(lookup$fgs_dbuddy_hg=="BS15_AU_PM02-a_files_1-104_rw_hg"),]

lookup$fgs_dbuddy_hg2=toupper(lookup$fgs_dbuddy_hg2)

lookup$visible_freq=1024
lookup$visible_freq[which(lookup$fgs_dbuddy_hg2=="BN")]=2048

for(i in 1:nrow(lookup)){

  test_fg=data_pull(paste("SELECT bins.*,soundfiles.DateTime,soundfiles.deployments_name FROM bins JOIN bins_filegroups ON bins.id = bins_filegroups.bins_id JOIN filegroups ON bins_filegroups.FG_name = filegroups.Name JOIN soundfiles ON bins.FileName= soundfiles.Name WHERE filegroups.Name = '",lookup$fgs_dbuddy_hg[i],"';",sep=""))

  format_fg_query = data.frame(as.POSIXct(test_fg$DateTime,tz='utc'),test_fg$deployments_name,test_fg$SegStart,test_fg$SegStart+test_fg$SegDur)
  #so to associate to db, need to lookup by date, mooring id,

  colnames(format_fg_query) = c("soundfiles.datetime","data_collection.name","bins.seg_start","bins.seg_end")
  #Here is a template to do this sort of thing:


  out = table_dataset_lookup(con,"SELECT bins.*,soundfiles.datetime,data_collection.name,a,b,c,d FROM bins JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id"
                             ,format_fg_query,c("timestamp","character varying","DOUBLE PRECISION","DOUBLE PRECISION"),return_anything = TRUE)

  out$id = as.integer(out$id)

  #check to see if any bins duplicated

  if(any(duplicated(out[,-which(colnames(out) %in% c("id","type"))]))){

    dupset = out[which(duplicated(out[,-which(colnames(out) %in% c("id","type"))]) | duplicated(out[,-which(colnames(out) %in% c("id","type"))],fromLast = TRUE)),]

    type_prefer = as.integer(names(table(out$type)))[which.max(table(out$type))]

    if(all(table(dupset$type) - max(table(dupset$type))==0) & type_prefer %in% dupset$type){

      remove = as.integer(dupset$id[which(dupset$type!= type_prefer)])

      out = out[-which(out$id %in% remove),]

    }else{
      stop("case not defined")
    }


  }

  #submit effort

  efforttab = data.frame(lookup$fgs_dbuddy_hg[i],"high grade semi-random","ground truth data used in analysis of data with detector positives from 1st gen lm analysis")
  colnames(efforttab)=c('name',"sampling_method","description")
  dbAppendTable(con,"effort",efforttab)

  #get id

  newid = dbFetch(dbSendQuery(con,paste("SELECT id FROM effort WHERE name ='",lookup$fgs_dbuddy_hg[i],"'",sep="")))

  #looks like it worked. so, just associate ids
  fg_ids = out$id
  fg_tab = data.frame(fg_ids,as.integer(newid$id))
  colnames(fg_tab)=c("bins_id","effort_id")
  dbAppendTable(con,"bins_effort",fg_tab)


  #submit detections
  tempout = paste(getwd(),"test.csv.gz",sep="/")
  system(paste("dbuddy pull detections ",tempout," --FileGroup ",lookup$fgs_dbuddy_hg[i]," --SignalCode ",lookup$fgs_dbuddy_hg2[i]," --Type i_neg",sep="")) #--Analysis_ID 10
  temp =read.csv(tempout)
  file.remove(tempout)

  if(any(duplicated(temp[,2:length(temp)]))){

    temp = temp[-which(duplicated(temp[,2:length(temp)])),]

  }

  #function to convert from dbuddy to pgpamdb
  out2 = dbuddy_pgpamdb_det_rough_convert(con,temp,procedure = 10,strength=2)

  if(length(unique(out2$analyst))>1){
    analyst_ = 2
  }else{
    analyst_=out2$analyst[1]
  }

  if(nrow(temp)>0){
    vf_ = temp$VisibleHz[1]
  }else{
    vf_ = lookup$visible_freq[i]
  }

  #now, need a function to interpolate negatives
  out3 = i_neg_interpolate(out2,out,vf_,10,lookup_from_match(con,'signals',lookup$fgs_dbuddy_hg2[i],'code')$id,analyst_)

  #combine with positive data:

  out_all = rbind(out2,out3)

  if(any(is.na(out_all$analyst))){
    stop("analyst not found in personnel")
  }

  #upload i_neg data
  dbAppendTable(con,"detections",out_all)

}

#DELETE FROM detections WHERE id IN (SELECT id from detections JOIN bins_detections ON bins_detections.detections.id
#                                    = bins_detections.bins_id JOIN bins_effort ON bins_detections.bins_id = bins_effort.bins_id JOIN
#                                    effort on bins_effort.effort_id = effort.id WHERE effort.name = 'AW13_AU_PH01_files_All_bn_hg')

#going through species by species for compleness. finishing out gs... gs done! BN done. RW done.

#so, what else is even on dbuddy that is worth transfering over?
#lm gt. transferring now
#- going to upload lm directly, so I can apply similar bin level n to fins
#- going to upload the lm cole review sections one by one so I can

#don't actually get it from here- soundfile ambiguity present
alldfo_deploy = data_pull("SELECT * FROM detections WHERE Analysis_ID IN (1,2,3);")
alldfo_deploy = alldfo_deploy[which(alldfo_deploy$Type=="DET"),]

#get it from here (# of rows match, and looks like a more original set)

alldfo_deploy2=read.csv("C:/Users/daniel.woodrich/Desktop/database/GS data upload/detections.csv")
alldfo_deploy2 = alldfo_deploy2[which(alldfo_deploy2$Type=="DET"),]

#need to fix labels (no 'strong maybe')

alldfo_deploy2$label[which(alldfo_deploy2$label=="sm")] = 'm'

colnames(alldfo_deploy2)[14]='data_collection.name'

RW_set = alldfo_deploy2[which(alldfo_deploy2$SignalCode =="RW"),]
GS_set = alldfo_deploy2[which(alldfo_deploy2$SignalCode =="GS"),]
out_RW = dbuddy_pgpamdb_det_rough_convert(con,RW_set,procedure = 4,strength=2,depnames=RW_set$data_collection.name)
out_GS = dbuddy_pgpamdb_det_rough_convert(con,GS_set,procedure = 16,strength=2,depnames=GS_set$data_collection.name)

dbAppendTable(con,"detections",out_RW)
dbAppendTable(con,"detections",out_GS)
