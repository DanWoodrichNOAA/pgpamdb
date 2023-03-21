upload_from_oldold <- function(conn,rav_og_data,fgname){

  #load in raven file from path.

  fg= dbFetch(dbSendQuery(con,paste("SELECT bins.*,soundfiles.name,soundfiles.duration FROM soundfiles JOIN bins ON bins.soundfiles_id = soundfiles.id JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON effort.id = bins_effort.effort_id WHERE effort.name = '",fgname,"'",sep="")))

  #rav_og_data = read.delim(bb_path)

  #put fg in cons order

  fg = fg[order(fg$name,fg$seg_start),]

  fg$cons = c(0,cumsum(fg$seg_end-fg$seg_start)[1:length(cumsum(fg$seg_end-fg$seg_start))-1])

  rav_og_data$dur = rav_og_data$End.Time..s.-rav_og_data$Begin.Time..s.

  rav_og_data$start_file = fg$soundfiles_id[findInterval(rav_og_data$Begin.Time..s.,fg$cons)]
  rav_og_data$end_file = fg$soundfiles_id[findInterval(rav_og_data$End.Time..s.,fg$cons)]

  #no dets overlap files for this one.. all(rav_og_data$start_file==rav_og_data$end_file)

  file_uniques = fg[which(fg$seg_start==0),]
  file_uniques$cons = c(0,cumsum(file_uniques$duration)[1:nrow(file_uniques)-1])

  rav_og_data$offset =rav_og_data$Begin.Time..s. - file_uniques$cons[findInterval(rav_og_data$Begin.Time..s.,file_uniques$cons)]

  #think I have everything we need. load it in as dets.

  template = dbFetch(dbSendQuery(con,'SELECT * FROM detections LIMIT 1'))

  input = data.frame(rav_og_data$offset,rav_og_data$offset+rav_og_data$dur,rav_og_data$Low.Freq..Hz.,rav_og_data$High.Freq..Hz.,
                     rav_og_data$start_file,rav_og_data$end_file,NA,"",10,1,6,2)

  colnames(input)=colnames(template)[2:(length(input)+1)]

  dbAppendTable(con,'detections',input)

}

#don't submit, trivial to submit data when originates with a query.
submit_fg<-function(con,fgdata,name,sampling_method,description,insert_nonmatching = FALSE){

  #if datetime isn't provided, add from filename
  if(!"DateTime" %in% colnames(fgdata)){

    #assumes normally named soundfiles.
    fgdata$DateTime =as.POSIXct(substr(fgdata$FileName,nchar(fgdata$FileName)-16,nchar(fgdata$FileName)-4),format = "%y%m%d-%H%M%S",tz="utc")
  }

  if(!"deployments_name" %in% fgdata){
    colnames(fgdata)[which(colnames(fgdata)=="Deployment")]="deployments_name"
  }

  format_fg_query = data.frame(as.POSIXct(fgdata$DateTime,tz='utc'),fgdata$deployments_name,fgdata$SegStart,fgdata$SegStart+fgdata$SegDur)
  #so to associate to db, need to lookup by date, mooring id,

  colnames(format_fg_query) = c("soundfiles.datetime","data_collection.name","bins.seg_start","bins.seg_end")
  #Here is a template to do this sort of thing:\

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

  #then, check to see if any bins need to first be submitte

  if(nrow(out)!=nrow(format_fg_query) & insert_nonmatching){

    stop("once I encounter this, write the bins insert here")
  }
  #submit effort

  efforttab = data.frame(name,sampling_method,description)
  colnames(efforttab)=c('name',"sampling_method","description")
  dbAppendTable(con,"effort",efforttab)

  #get id

  newid = dbFetch(dbSendQuery(con,paste("SELECT id FROM effort WHERE name ='",name,"'",sep="")))

  #looks like it worked. so, just associate ids
  fg_ids = out$id
  fg_tab = data.frame(fg_ids,as.integer(newid$id))
  colnames(fg_tab)=c("bins_id","effort_id")
  bins_loaded = dbAppendTable(con,"bins_effort",fg_tab)

  return(bins_loaded)

}
#bin_negatives(data,fg,bintype="LOW",analyst='previous',procedure = 5,signal_code =3,format='db')


dbGet <-function(x){
  x = gsub("[\r\n]", "", x)
  dbFetch(dbSendQuery(con,x))
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
fgs_dbuddy_hg = fgs_dbuddy[which(grepl("train_GSex",fgs_dbuddy$Name)),]
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

  efforttab = data.frame(lookup$fgs_dbuddy_hg[i],"hand","Used to evaluate and train GS detector on DFO data. Contains no gunshot calls")
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
  out2 = detx_pgpamdb_det_rough_convert(con,temp,procedure = 10,strength=2)

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
out_RW = detx_pgpamdb_det_rough_convert(con,RW_set,procedure = 4,strength=2,depnames=RW_set$data_collection.name)
out_GS = detx_pgpamdb_det_rough_convert(con,GS_set,procedure = 16,strength=2,depnames=GS_set$data_collection.name)

dbAppendTable(con,"detections",out_RW)
dbAppendTable(con,"detections",out_GS)

#alright- checked a lot of data after loading
#some work to do. easiest to harder.

#i_neg bug meant all i_neg types must be redone.

#first issue is there were some dupcliate detections in procedure 9. should be super easy to clean up.

dets = dbFetch(dbSendQuery(con,"SELECT * FROM detections WHERE procedure = 14"))
dups = which(duplicated(dets[,-which(colnames(dets) %in% c('id',"original_id"))]))
del_ids = dets$id[dups]

table_delete(con,'detections',as.integer(del_ids),hard_delete = TRUE)

#next, need to upload the GSex detections. Just upload the yeses, not i_neg, I will redo them in a loop along with the others.



#how to locate in dbuddy?

test = data_pull("SELECT DISTINCT Analysis_ID FROM detections WHERE Type = 'i_neg' and SignalCode = 'GS';")

#need to filter by 'dfo data' . I should put a column in data_collection to represent where the data are sourced from.

moorings = dbFetch(dbSendQuery(con,"SELECT name FROM data_collection WHERE institution_source = 'DFO'"))

#for dbuddy query, join moorings to soundfiles and then join to detections.
query = paste("SELECT DISTINCT detections.*,deployments.Name FROM deployments JOIN soundfiles ON deployments.Name = soundfiles.deployments_name JOIN
              detections ON detections.StartFile = soundfiles.Name
              WHERE Type = 'i_neg' AND deployments_name IN ('",paste(moorings$name,collapse="','"),"');",sep="")

query <- gsub("[\r\n]", "", query)

gsexgt = data_pull(query)

gsexgt$SignalCode="GS"

out = detx_pgpamdb_det_rough_convert(con,gsexgt,17,2,depnames = gsexgt$Name)

#don't bother to calculate i_neg, for now.

dbAppendTable(con,"detections",out)

#just to keep things clean, I want to delete all i_neg negatives right now so I can assume all GT has no i_neg.
#add the flag to retain negatives in i_neg procedures with comments, so that this info isn't lost.
test_del = dbFetch(dbSendQuery(con,"SELECT COUNT(*) FROM detections WHERE procedure IN (10,12,13) AND label = 0 AND comments =''"))

i_neg_all_noComments = dbFetch(dbSendQuery(con,"SELECT id FROM detections WHERE procedure IN (10,12,13) AND label = 0 AND comments =''"))
i_neg_all_noComments$id = as.integer(i_neg_all_noComments$id)

#delete all of it.
table_delete(con,'detections',i_neg_all_noComments$id,hard_delete = TRUE)

#check that i_neg was deleted. Looks good from a quick glance.

#now, load in rest of 10 gt fgs, and then data.
#what signals have been loaded in already?
dbFetch(dbSendQuery(con,"SELECT DISTINCT signal_code FROM detections WHERE procedure = 10"))
#RW,GS,LM,BN
#what remains?
#BB,FN,RB
#other ones that are possibly recoverable
#MX
#ones that are worthless (BP 'bumps?')

#identify the gts.

fgpath = "C:/Users/daniel.woodrich/Desktop/database/FileGroups"

names = dir(fgpath)
names2 = names[which(grepl("bb",names)|grepl("fn",names)|grepl("rb",names))]
#check to make sure not loaded up yet.
names2 = substr(names2,1,nchar(names2)-4)

#none seem to be loaded
all_loadedfgs = dbFetch(dbSendQuery(con,"SELECT name FROM effort"))

namestab = data.frame(names2,0)

colnames(namestab)[2]= "splitsize"

namestab$splitsize[grep('bb',namestab$names2)]=300
namestab$splitsize[grep('fn',namestab$names2)]=300
namestab$splitsize[grep('rb',namestab$names2)]=90

namestab$sampling_method = ""
namestab$description = ""

namestab$sampling_method[grep('bb',namestab$names2)]="high grade semi-random"
namestab$sampling_method[grep('fn',namestab$names2)]="high grade semi-random"
namestab$sampling_method[grep('rb',namestab$names2)]="high grade"


namestab$description[grep('bb',namestab$names2)]="ground truth data used for detector training, negatives provided but assumed from boxes."
namestab$description[grep('fn',namestab$names2)]="ground truth data used for detector training, negatives provided but assumed from boxes."
namestab$description[grep('rb',namestab$names2)]="ground truth data used for detector training, negatives provided but assumed from boxes. original labels provided by Heloise in 2020. label source: https://doi.org/10.1007/s00300-019-02462-y"

#make a function to do this.

for(i in 1:length(names2)){

    fg = read.csv(paste(fgpath,"/",names2[i],".csv",sep=""))

    fg$Type=NULL

    print(head(fg))

    #standard split (results in fewer custom bins)

    fg_broke = fg_breakbins(fg,namestab$splitsize[i])

    print(head(fg_broke))

    submit_fg(con,fg_broke,namestab$names2[i],namestab$sampling_method[i],namestab$description[i])
}

#cool. ribbon gt looks bugged/incorrect, so instead just go with the original gt labels. I recall we changed them a little if at all.

ribbon_gt = read.csv("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/RibbonDownsweep/FormatDataWavs_EditBS12_AU_04b.csv")

#format into detx format

ribbon_gt$Soundfile = paste(substr(ribbon_gt$Soundfile,nchar(ribbon_gt$Soundfile)-27,nchar(ribbon_gt$Soundfile)-19),substr(ribbon_gt$Soundfile,nchar(ribbon_gt$Soundfile)-16,nchar(ribbon_gt$Soundfile)),sep="")

#character replace old wav names with new

#standardize old mooring names:

ribbon_gt$Full.Mooring.Name=gsub('BS12_AU_04b1','BS12_AU_04b',ribbon_gt$Full.Mooring.Name)
ribbon_gt$Full.Mooring.Name=gsub('BS12_AU_04b2','BS12_AU_04b',ribbon_gt$Full.Mooring.Name)
ribbon_gt$Full.Mooring.Name=gsub('BS12_AU_04b2','BS12_AU_04b',ribbon_gt$Full.Mooring.Name)
ribbon_gt$Full.Mooring.Name=gsub('BS12_AU_05a','BS12_AU_05b',ribbon_gt$Full.Mooring.Name)




name_lookup =lookup_from_match(con,'data_collection',unique(ribbon_gt$Full.Mooring.Name),'historic_name',idname = 'id')
cur_names = dbFetch(dbSendQuery(con,paste("SELECT name,historic_name FROM data_collection WHERE id IN (",paste(name_lookup$id,collapse = ",",sep=""),")",sep="")))

ribbon_gt$newname = cur_names$name[match(ribbon_gt$Full.Mooring.Name, cur_names$historic_name)]

#unique(substr(ribbon_gt$Soundfile,nchar(ribbon_gt$Soundfile)-27,nchar(ribbon_gt$Soundfile)-15))
testdat = ribbon_gt[-which(duplicated(substr(ribbon_gt$Soundfile,nchar(ribbon_gt$Soundfile)-27,nchar(ribbon_gt$Soundfile)-15))),]

ribbon_gt$Soundfile=gsub('_','-',ribbon_gt$Soundfile)

ribbon_gt$Soundfile=gsub('BS04b','BSPM04',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('BS05b','BSPM05',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('RW02','RWBS02',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('RW03','RWBS03',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('BS02b','BSPM02_b',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('U-AWBS03','AU-AWBS03',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('BS05a','BSPM05',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('U-AWBS02','AU-AWBS02',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('U-AWBS01','AU-AWBS01',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('U-AWKZ01','AU-AWKZ01',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('U-AWPH01','AU-AWPH01',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('U-AWBF02','AU-AWBF02',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('U-AWBF03','AU-AWBF03',ribbon_gt$Soundfile)
ribbon_gt$Soundfile=gsub('AU-BS08a','AU-BSPM08',ribbon_gt$Soundfile)


ribbon_gt$Soundfile=gsub('\\\\','',ribbon_gt$Soundfile)

rgt_detx = data.frame(ribbon_gt$Left.time..sec.,ribbon_gt$Right.time..sec.,ribbon_gt$Bottom.freq..Hz.,ribbon_gt$Top.freq..Hz.,
                      ribbon_gt$Soundfile,ribbon_gt$Soundfile,NA,8192,"y","","RB","i_neg",10,"HFM",ribbon_gt$newname)

colnames(rgt_detx) = c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","probs","VisibleHz","label","Comments",
                       "SignalCode","Type","Analysis_ID","LastAnalyst","Name")

#test = vector_validate(con,'soundfiles','name',rgt_detx$StartFile)
db_dat = detx_pgpamdb_det_rough_convert(con,rgt_detx,10,2,rgt_detx$Name)

dbAppendTable(con,'detections',db_dat)

#final 'easy' gt data to load in- fn and bb.load them in a big loop, convert, insert.

fndat_all = list()
for(i in 1:length(dir("C:/Users/daniel.woodrich/Desktop/database/GroundTruth/FN"))){
  fndat = read.csv(paste("C:/Users/daniel.woodrich/Desktop/database/GroundTruth/FN",dir("C:/Users/daniel.woodrich/Desktop/database/GroundTruth/FN"),sep="/")[i])
  fndat_all[[i]]=fndat
}

fndat_all = do.call('rbind',fndat_all)

bbdat_all = list()
for(i in 1:length(dir("C:/Users/daniel.woodrich/Desktop/database/GroundTruth/BB"))){
  bbdat = read.csv(paste("C:/Users/daniel.woodrich/Desktop/database/GroundTruth/BB",dir("C:/Users/daniel.woodrich/Desktop/database/GroundTruth/BB"),sep="/")[i])
  bbdat_all[[i]]=bbdat
}

bbdat_all = do.call('rbind',bbdat_all)

fwgt_all = rbind(fndat_all,bbdat_all)

fwgt_add_cols = data.frame(fwgt_all[,c(1:6)],NA,64,fwgt_all$label,"",fwgt_all$SignalCode,fwgt_all$Type,99999,"DFW")

colnames(fwgt_add_cols) = c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","probs","VisibleHz","label","Comments",
                       "SignalCode","Type","Analysis_ID","LastAnalyst")

#update old naming convention:

fwgt_add_cols$StartFile=gsub('EA-RW01','EA-RWUM01',fwgt_add_cols$StartFile)
fwgt_add_cols$EndFile=gsub('EA-RW01','EA-RWUM01',fwgt_add_cols$EndFile)


#convert to db format
#test = vector_validate(con,'soundfiles','name',fwgt_add_cols$StartFile)
db_dat_fw = detx_pgpamdb_det_rough_convert(con,fwgt_add_cols,10,2)

dbAppendTable(con,'detections',db_dat_fw)

#load in the cole boxes. I should review these after to ensure consistency with our consensus.

path = "//nmfs/akc-nmml/CAEP/Acoustics/Projects/DFO RW/Upcall_annotations/bouts_review/cole selection tables flat dir extra measurements for convenience"

dfo_upcall_gt = list()

for(n in 1:length(dir(path))){

  dets = read.delim(paste(path,"/",dir(path)[n],sep=""))

  comments = dets[,which(colnames(dets) == "comments" | colnames(dets) == "Additional.analysis" | colnames(dets) == "Notes")]
  durs = dets[,"End.Time..s."] - dets[,"Begin.Time..s."]

  dfo_upcall_gt[[n]] = data.frame(dets$File.Offset..s., dets$File.Offset..s. + durs, dets$Low.Freq..Hz., dets$High.Freq..Hz.,
                                  dets$Begin.File,dets$End.File,comments)

  colnames(dfo_upcall_gt[[n]]) = c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","comments")

}

dfo_upcall_gt = do.call('rbind',dfo_upcall_gt)

dfo_upcall_gt$comments[which(is.na(dfo_upcall_gt$comments))]=""

#turn the 'maybes' into labels, and all unmarked into yeses

dfo_upcall_gt$label[which(dfo_upcall_gt$comments=='maybe')] = 'm'

dfo_upcall_gt$label[which(is.na(dfo_upcall_gt$label))]="y"

dfo_upcall_gt$SignalCode = "RW"

colnames(dfo_upcall_gt)[which(colnames(dfo_upcall_gt)=='comments')]="Comments"
#now redundant
dfo_upcall_gt$Comments = ""

dfo_upcall_gt$LastAnalyst = "CTW"
dfo_upcall_gt$probs = NA

dfo_upcall_gt_dbform = detx_pgpamdb_det_rough_convert(con,dfo_upcall_gt,10,2)
dfo_upcall_gt_dbform$probability =NA

dbAppendTable(con,'detections',dfo_upcall_gt_dbform)

#found a couple duplicates. removed

#continue this script to handle two cases, ildiko ribbon deployment and low moan initial deployment. keeping them in here since I
#want to easily use and refine the functions defined in here temporarily as well as trial new ones.

rb_data_path = "//akc0ss-n086/NMML_CAEP_Acoustics/Detector/RibbonDownsweep/Ildiko work folder/Restructured Deployment Review Showing Effort"

#loop through this and load data into memory
FGlist = list()
GTlist = list()

counter = 0

for(i in 1:length(dir(rb_data_path))){
  for(n in 1:length(dir(dir(rb_data_path,full.names = TRUE)[i]))){

    counter = counter + 1

    infiles = dir(dir(dir(rb_data_path,full.names = TRUE)[i],full.names = TRUE)[n],full.names = TRUE)

    fgfile = read.csv(infiles[grep("SFiles",infiles)])
    source = basename(infiles[grep("SFiles",infiles)])
    fgfile$Site = NULL
    FGlist[[counter]] = fgfile

    gts_comb = list()

    for(p in 1:(length(infiles)-1)){

      gtin= read.delim(infiles[-grep("SFiles",infiles)][p])

      gtin$filedur = fgfile$Duration[match(gtin$File,fgfile$SFsh)]

      gtin$dur = gtin$End.Time..s.-gtin$Begin.Time..s.

      gtin$endtime_offset = gtin$FileOffsetBegin + gtin$dur

      gtin$endfile = gtin$File

      while(any(gtin$endtime_offset>gtin$filedur)){
        #for(q in sum(gtin$endtime_offset>gtin$filedur)){

        #= fgfile$SFsh[which(fgfile$SFsh == gtin[which(gtin$endtime_offset>gtin$filedur),"File"] )]

        filevec = gtin[which(gtin$endtime_offset>gtin$filedur),"endfile"]

        gtin[which(gtin$endtime_offset>gtin$filedur),"endfile"] = fgfile$SFsh[match(filevec,fgfile$SFsh)+1]

        gtin[which(gtin$endtime_offset>gtin$filedur),"endtime_offset"] = gtin[which(gtin$endtime_offset>gtin$filedur),"endtime_offset"] - fgfile$Duration[match(filevec,fgfile$SFsh)]

        #}

      }

      gts_comb[[p]] = gtin

    }

    GTlist[[counter]] = do.call('rbind',gts_comb)

  }

}

allfg = do.call('rbind',FGlist)
#format to instinct format

#allfgstarttime = as.POSIXct(substr(allfg$SFsh,nchar(allfg$SFsh)-16,nchar(allfg$SFsh)-4),format = "%y%m%d-%H%M%S",tz='utc')

moornames = gsub("IlSub","",allfg$MooringName)
moornames = gsub("Ilsub","",moornames)

namelookup = lookup_from_match(con,'data_collection',moornames,'historic_name')

newnames = dbFetch(dbSendQuery(con,paste("SELECT id,name FROM data_collection WHERE id IN (",paste(namelookup$id,collapse=",",sep=""),")",sep="")))
newnames$id = as.integer(newnames$id)
allnamelookup = merge(namelookup,newnames,by = 'id')


fgformat = data.frame(allfg$SFsh,"","",allfg$Duration,allnamelookup$name[match(moornames,allnamelookup$historic_name)],0,allfg$Duration)
colnames(fgformat) = c("FileName","FullPath","StartTime","Duration","Deployment","SegStart","SegDur")

#to avoid issues, want to get the durations which match db before sending it through. and suspect some soundfiles need to get renamed

fgformat$FileName = gsub('AU-BS08a','AU-BSPM08',fgformat$FileName)
fgformat$FileName = gsub('AU-BS02b','AU-BSPM02_b',fgformat$FileName)
fgformat$FileName = gsub('AU-BS04a','AU-BSPM04',fgformat$FileName)
fgformat$FileName = gsub('AU-BS05a','AU-BSPM05',fgformat$FileName)
fgformat$FileName = gsub('AU-CZB03','AU-CZIC02_03',fgformat$FileName)

#test = vector_validate(con,'soundfiles','name',fgformat$FileName)

tempds = data.frame(fgformat$FileName)
colnames(tempds)="name"

#looks the same! no modifying this required.
realdurs = table_dataset_lookup(con,"SELECT name,duration FROM soundfiles",tempds,"character varying")

#split into correct size bins.
fgformat_split = fg_breakbins(fgformat,90)


#submit_fg(con,fgformat_split,'Ildiko_alldep',"procedural","april and may sections used for detector deployment, combined into one large fg.")

#great, looks like it's in!

#format to detx format
allgt = do.call('rbind',GTlist)
allgt_format = data.frame(allgt$FileOffsetBegin,allgt$endtime_offset,allgt$Low.Freq..Hz.,allgt$High.Freq..Hz.,
                          allgt$File,allgt$endfile,allgt$RB.prob,2048,allgt$True.Positive..y.,"",'RB',"DET",9999,"IK")

colnames(allgt_format) = c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","probs","VisibleHz","label","Comments",
                       "SignalCode","Type","Analysis_ID","LastAnalyst")

#need to format sfs for gt as well...
#test = vector_validate(con,'soundfiles','name',all_rb_dets$start_file)
allgt_format$StartFile = gsub('AU-BS08a','AU-BSPM08',allgt_format$StartFile)
allgt_format$StartFile = gsub('AU-BS02b','AU-BSPM02_b',allgt_format$StartFile)
allgt_format$StartFile = gsub('AU-BS04a','AU-BSPM04',allgt_format$StartFile)
allgt_format$StartFile = gsub('AU-BS05a','AU-BSPM05',allgt_format$StartFile)
allgt_format$StartFile = gsub('AU-CZB03','AU-CZIC02_03',allgt_format$StartFile)

allgt_format$EndFile = gsub('AU-BS08a','AU-BSPM08',allgt_format$EndFile)
allgt_format$EndFile = gsub('AU-BS02b','AU-BSPM02_b',allgt_format$EndFile)
allgt_format$EndFile = gsub('AU-BS04a','AU-BSPM04',allgt_format$EndFile)
allgt_format$EndFile = gsub('AU-BS05a','AU-BSPM05',allgt_format$EndFile)
allgt_format$EndFile = gsub('AU-CZB03','AU-CZIC02_03',allgt_format$EndFile)


#add py's to dets.

#come up with a table of times to discard. criteria is after n, but within the hour, is unknown, not yes or no, so remove these.

allgt_format$DateTime = as.POSIXct(substr(allgt_format$StartFile,nchar(allgt_format$StartFile)-16,nchar(allgt_format$StartFile)-4),format="%y%m%d-%H%M%S",tz='utc')


allgt_format$hr_subset = substr(allgt_format$StartFile,1,nchar(allgt_format$StartFile)-8)

label_table = data.frame(table(allgt_format$hr_subset,allgt_format$label))

label_table_y = label_table[which(label_table$Var2=='y' & label_table$Freq>=3),]

#loop through above, locate the highest start time yes, and discard the n bins after that

#not only that, but assign the blank labels in here as well. by procedure, blanks following the final y will be marked as uk.
#at the very end,

allgt_format$tempid = 1:nrow(allgt_format)

for(i in 1:nrow(label_table_y)){

  allgt_format_sub = allgt_format[which(allgt_format$hr_subset==label_table_y$Var1[i]),]

  allgt_format_sub$DateTimePrecise = allgt_format_sub$DateTime + as.numeric(allgt_format_sub$StartTime)

  last_yes= max(allgt_format_sub[which(allgt_format_sub$label=="y"),"DateTimePrecise"])

  #change all of the blank dets following last yes to py.

  upgradeids = allgt_format_sub[which(allgt_format_sub$label=="" & allgt_format_sub$DateTimePrecise >= last_yes),"tempid"]

  if(length(upgradeids)!=0){

    allgt_format[which(allgt_format$tempid %in% upgradeids),"label"]="py"
  }

}

allgt_format$DateTime=NULL
allgt_format$hr_subset=NULL
allgt_format$tempid=NULL
#get the bin negatives

gt_bin_negs = bin_negatives(allgt_format,fgformat_split,"SHI","IK")




all_rb_dets = rbind(allgt_format,gt_bin_negs)


#not super sure how reliable the 'py' will be, but try it out. can always attempt again.

#the rest of the blanks become n's

all_rb_dets[which(all_rb_dets$label==""),"label"]="n"
#bug correct
all_rb_dets[which(all_rb_dets$label=="nn"),"label"]="n"


all_rb_dets$label[which(is.na(all_rb_dets$label))]="n"

format_rb = detx_pgpamdb_det_rough_convert(con,all_rb_dets,18,2)

#dbAppendTable(con,'detections',format_rb)

#submitted!

#lastly for uploads - time to upload cole 1st analysis data.
#plan is-
#load in 'coledata' and 'dandata'

coledata = read.csv("C:/Users/daniel.woodrich/Desktop/database/input_data_to_db/ColeData.csv")
dandata = read.csv("C:/Users/daniel.woodrich/Desktop/database/input_data_to_db/DanData.csv")

#don't overcomplicate, just submit the 'final' consensus b/t me and cole.
alldata = rbind(coledata[-which(coledata$tempID %in% dandata$tempID),],dandata)

allnames = dbFetch(dbSendQuery(con,"SELECT name,historic_name FROM data_collection"))

alldata$newname = alldata$Mooring
alldata$newname[which(alldata$newname %in% allnames$historic_name)] = allnames$name[match(alldata$newname[which(alldata$newname %in% allnames$historic_name)],allnames$historic_name)]

#remove known bugged sections (that one m4 one, find it)
#remove bugged mooring

alldata = alldata[-which(alldata$newname == "BS15_AU_PM04"),]

alldata$label[which(is.na(alldata$label) | alldata$label == "")]='n'

#have to get the filegroup to run with bin negatives.

moorings = paste(unique(alldata$newname),collapse="','",sep="")

query = gsub("[\r\n]", "",paste("SELECT soundfiles.name,'','',duration,data_collection.name,0,600 FROM soundfiles JOIN data_collection ON data_collection.id
                                  = soundfiles.data_collection_id WHERE data_collection.name IN ('",moorings,"')",sep=""))

allfg = dbFetch(dbSendQuery(con,query))

colnames(allfg) =c("FileName"  ,"FullPath" ,"StartTime","Duration" ,"Deployment","SegStart"   ,"SegDur")

#lots of file prefixes to correct. probably can mine them using new name.

prefix_lookup = dbFetch(dbSendQuery(con,"SELECT DISTINCT ON (data_collection_id) data_collection.name,soundfiles.name FROM soundfiles JOIN data_collection ON soundfiles.data_collection_id = data_collection.id"))

sttimes = substr(alldata$StartFile,nchar(alldata$StartFile)-16,nchar(alldata$StartFile)-4)
edtimes = substr(alldata$EndFile,nchar(alldata$EndFile)-16,nchar(alldata$EndFile)-4)

file_ex = prefix_lookup$name..2[match(alldata$newname,prefix_lookup$name)]

alldata$StartFile = paste(substr(file_ex,1,nchar(file_ex)-17),sttimes,".wav",sep="")
alldata$EndFile = paste(substr(file_ex,1,nchar(file_ex)-17),edtimes,".wav",sep="")

alldata$tempID=NULL
alldata$Mooring=NULL
alldata$newname=NULL

alldata_negs = bin_negatives(alldata,allfg,"LOW","CTW")

alldata_wnegs = rbind(alldata,alldata_negs)

format_alldata_wnegs = detx_pgpamdb_det_rough_convert(con,alldata_wnegs,5,2)

format_alldata_wnegs[which(is.na(format_alldata_wnegs$label)),"label"]=20
format_alldata_wnegs$signal_code= 3
#run the bin negatives on it in low assumption.

format_alldata_wnegs[which(format_alldata_wnegs$label==20),"high_freq"]=64
#upload all to db, check and rework if necessary.
dbAppendTable(con,'detections',format_alldata_wnegs)

#############
#next, going to just dl the effort and procedures table, and manually create the
#effort_procedures table.

effort = dbFetch(dbSendQuery(con,"SELECT * FROM effort"))
effort = effort[order(effort$id),]
procedures = dbFetch(dbSendQuery(con,"SELECT * FROM procedures"))
procedures = procedures[order(procedures$id),]


#make temporary working folder
path = "C:/Users/daniel.woodrich/Downloads/temp"

write.csv(effort,paste(path,"effort.csv",sep="/"))
write.csv(procedures,paste(path,"procedures.csv",sep="/"))
#read in the new table
newtab = read.delim(paste(path,"effort_procedures.csv",sep="/"))

#upload to db

dbAppendTable(con,'effort_procedures',newtab)

#found i was missing round1_pull1 fg from db. pull it from dbuddy and add.


fg=data_pull("SELECT bins.*,soundfiles.DateTime,soundfiles.deployments_name FROM bins JOIN bins_filegroups ON bins.id = bins_filegroups.bins_id JOIN filegroups ON bins_filegroups.FG_name = filegroups.Name JOIN soundfiles ON bins.FileName= soundfiles.Name WHERE filegroups.Name = 'round1_pull1';")

format_fg_query = data.frame(as.POSIXct(fg$DateTime,tz='utc'),fg$deployments_name,fg$SegStart,fg$SegStart+fg$SegDur)
#so to associate to db, need to lookup by date, mooring id,

colnames(format_fg_query) = c("soundfiles.datetime","data_collection.name","bins.seg_start","bins.seg_end")
#Here is a template to do this sort of thing:


out = table_dataset_lookup(con,"SELECT bins.*,soundfiles.datetime,data_collection.name,a,b,c,d FROM bins JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id"
                           ,format_fg_query,c("timestamp","character varying","DOUBLE PRECISION","DOUBLE PRECISION"),return_anything = TRUE)

oddbins = format_fg_query[-which(paste(format_fg_query$soundfiles.datetime,format_fg_query$data_collection.name) %in% paste(out$a,out$b)),]

#need to insert the odd bins.
#first, find the soundfile id

out2 = table_dataset_lookup(con,"SELECT soundfiles.*,a,b FROM soundfiles JOIN data_collection ON soundfiles.data_collection_id = data_collection.id"
                              ,oddbins[,1:2],c("timestamp","character varying"),return_anything = TRUE)

colnames(out2)[6:7] = c('soundfiles.datetime','data_collection.name')

oddbins_w_sfid = merge(oddbins,out2)

bins_add = data.frame(oddbins_w_sfid$id,oddbins_w_sfid$bins.seg_start,oddbins_w_sfid$bins.seg_end,0)
colnames(bins_add)=c("soundfiles_id","seg_start","seg_end","type")

dbAppendTable(con,'bins',bins_add)

out_after = table_dataset_lookup(con,"SELECT bins.*,soundfiles.datetime,data_collection.name,a,b,c,d FROM bins JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id"
                           ,format_fg_query,c("timestamp","character varying","DOUBLE PRECISION","DOUBLE PRECISION"),return_anything = TRUE)

#still 2 rows short- why?

comp1 = paste(format_fg_query$soundfiles.datetime,format_fg_query$data_collection.name,format_fg_query$bins.seg_start,format_fg_query$bins.seg_end)
comp2 = paste(out_after$datetime,out_after$name,out_after$seg_start,out_after$seg_end)

#found them, added back in similar to as before (interactively).

#alright, now add in the fg.

out = table_dataset_lookup(con,"SELECT bins.*,soundfiles.datetime,data_collection.name,a,b,c,d FROM bins JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id"
                           ,format_fg_query,c("timestamp","character varying","DOUBLE PRECISION","DOUBLE PRECISION"),return_anything = TRUE)

out$id = as.integer(out$id)

efforttab = data.frame("round1_pull1","high grade random","Random pull of humpback yes AFSC/soundchecker data from the singing season Oct 1st 2015 to March 1st 2016")
colnames(efforttab)=c('name',"sampling_method","description")
dbAppendTable(con,"effort",efforttab)

#get id

newid = dbFetch(dbSendQuery(con,"SELECT id FROM effort WHERE name ='round1_pull1'"))

#looks like it worked. so, just associate ids
fg_ids = out$id
fg_tab = data.frame(fg_ids,as.integer(newid$id))
colnames(fg_tab)=c("bins_id","effort_id")
dbAppendTable(con,"bins_effort",fg_tab)

#alright, it is in.

#Perform the airgun y review to correct incorrect FW determinations. query to create a filegroup.

#don't actually want this- would pull from SC data if it's in there at all.
#ag_fg= dbFetch(dbSendQuery(con,"SELECT bins.* FROM bins JOIN bin_label_wide ON bins.id = bin_label_wide.id WHERE ag IN (1,21) and type = 1"))

ag_fg= dbFetch(dbSendQuery(con,"SELECT DISTINCT bins.* FROM bins JOIN bins_detections ON bins.id = bins_detections.bins_id JOIN detections ON bins_detections.detections_id = detections.id WHERE detections.procedure = 8 AND detections.label IN (1,21) AND bins.type = 1"))

ag_fg_effort = data.frame("fw_gen1_ag","high grade","All LOW bins where airgun is yes or procedure yes. Queried for only AG data from 1st gen FW analysis. Reviewed to address the case where an airgun FP resulted in misclassification of FW by procedure")
colnames(ag_fg_effort)=c('name',"sampling_method","description")
dbAppendTable(con,"effort",ag_fg_effort)

#manually put together the rw_09 gt

fg= dbFetch(dbSendQuery(con,"SELECT bins.*,soundfiles.name FROM soundfiles JOIN bins ON bins.soundfiles_id = soundfiles.id JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON effort.id = bins_effort.effort_id WHERE effort.name = 'RW09_EA_UM01_files_1-75_bb_hg'"))

#alright, it looks like the fg is just wrong! Doesn't contain all of the bins from each sf as it should.

fgcor = dbFetch(dbSendQuery(con,paste("SELECT bins.*,soundfiles.name FROM soundfiles JOIN bins ON bins.soundfiles_id = soundfiles.id WHERE bins.type = 1 AND soundfiles.name IN ('",paste(fg$name,collapse="','",sep=""),"')",sep="")))
#register the others as the same fg.

fg_id = dbFetch(dbSendQuery(con,"SELECT id FROM effort WHERE name = 'RW09_EA_UM01_files_1-75_bb_hg'"))

newtab = data.frame(fgcor$id[-which(fgcor$id %in% fg$id)],rep(fg_id$id,75))
colnames(newtab) = c("bins_id","effort_id")
newtab$bins_id = as.integer(newtab$bins_id)
newtab$effort_id = as.integer(newtab$effort_id)

dbAppendTable(con,'bins_effort',newtab)

#try again

fg= dbFetch(dbSendQuery(con,"SELECT bins.*,soundfiles.name,soundfiles.duration FROM soundfiles JOIN bins ON bins.soundfiles_id = soundfiles.id JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON effort.id = bins_effort.effort_id WHERE effort.name = 'RW09_EA_UM01_files_1-75_bb_hg'"))


#before changing this, delete the current gt on there

query = "SELECT DISTINCT detections.* FROM detections JOIN bins_detections on bins_detections.detections_id = detections.id JOIN bins on bins.id = bins_detections.bins_id
        JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON effort.id = bins_effort.effort_id
        WHERE detections.status = 1 AND detections.procedure =10 AND detections.signal_code = 6 AND effort.name = 'RW09_EA_UM01_files_1-75_bb_hg'"

query = gsub("[\r\n]", "",query)

dets= dbFetch(dbSendQuery(con,query))

table_delete(con,'detections',dets$id,hard_delete = TRUE)

#now format old data to insert. here is the old gt.

rav_og_data = read.delim("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/RavenBLEDscripts/Data/Selection tables/BB/RW09_EA_01Sum/RW09_EA_01_files_1-75.txt")

#put fg in cons order

fg = fg[order(fg$name,fg$seg_start),]

fg$cons = c(0,cumsum(fg$seg_end-fg$seg_start)[1:length(cumsum(fg$seg_end-fg$seg_start))-1])

rav_og_data$start_file = fg$soundfiles_id[findInterval(rav_og_data$Begin.Time..s.,fg$cons)]
rav_og_data$end_file = fg$soundfiles_id[findInterval(rav_og_data$End.Time..s.,fg$cons)]

#no dets overlap files for this one.. all(rav_og_data$start_file==rav_og_data$end_file)

file_uniques = fg[which(fg$seg_start==0),]

rav_og_data$offset =rav_og_data$Begin.Time..s. - file_uniques$cons[findInterval(rav_og_data$Begin.Time..s.,file_uniques$cons)]
rav_og_data$dur = rav_og_data$End.Time..s.-rav_og_data$Begin.Time..s.

#think I have everything we need. load it in as dets.

template = dbFetch(dbSendQuery(con,'SELECT * FROM detections LIMIT 1'))

input = data.frame(rav_og_data$offset,rav_og_data$offset+rav_og_data$dur,rav_og_data$Low.Freq..Hz.,rav_og_data$High.Freq..Hz.,
                   rav_og_data$start_file,rav_og_data$end_file,NA,"",10,1,6,2)

colnames(input)=colnames(template)[2:(length(input)+1)]

dbAppendTable(con,'detections',input)

#cool, it's fixed!

#need to do a round of corrections- some dets identified where end time was greater than end file.
#bring in those which start time > start file dur.

query = "SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id WHERE detections.start_time > soundfiles.duration"

detss = dbFetch(dbSendQuery(con,query))

#need to calculate real time start of files.

detss$datetime
detss$det_datetime = detss$datetime + detss$start_time

#now the tricky part- find what should be the correct soundfile for each. Could just loop it... :)

min_sfs = c()
min_sfs_lens=c()
max_sfs = c()

for(i in 1:nrow(detss)){

  var1 = as.integer(dbFetch(dbSendQuery(con,paste("SELECT soundfiles.id FROM soundfiles WHERE soundfiles.datetime = (SELECT MIN(datetime) FROM soundfiles WHERE soundfiles.datetime > '",as.character(detss$datetime[i]),"' AND soundfiles.data_collection_id = ",as.integer(detss$data_collection_id[i]),") AND soundfiles.data_collection_id = ",as.integer(detss$data_collection_id[i]),sep="")))$id)

  var2 = as.integer(dbFetch(dbSendQuery(con,paste("SELECT soundfiles.id FROM soundfiles WHERE soundfiles.datetime = (SELECT MAX(datetime) FROM soundfiles WHERE soundfiles.datetime < '",as.character(detss$det_datetime[i]),"' AND soundfiles.data_collection_id = ",as.integer(detss$data_collection_id[i]),") AND soundfiles.data_collection_id = ",as.integer(detss$data_collection_id[i]),sep="")))$id)

  if(length(var1)==0){

    var1 = NA
  }

  if(length(var2)==0){
    var2= NA
  }

  min_sfs = c(min_sfs,var1)
  min_sfs_lens = c(min_sfs_lens,length(var1))

  max_sfs = c(max_sfs,var2)

  print(i)
  print(length(min_sfs[i]))
  print(length(min_sfs[i]))

  if(length(var1)>1){
    stop()
  }

}

detss$min_sfs = min_sfs
detss$max_sfs = max_sfs

#cool. now, make a lookup table.

lookup = lookup_from_match(con,"soundfiles",unique(c(detss$max_sfs,detss$min_sfs)[-which(is.na(c(detss$max_sfs,detss$min_sfs)))]),)

df = data.frame(unique(c(detss$max_sfs,detss$min_sfs)[-which(is.na(c(detss$max_sfs,detss$min_sfs)))]))
colnames(df)="id"

soundfiles = table_dataset_lookup(con,"SELECT * FROM soundfiles",df,"integer")

#the bad gt data look too messed up to be fixed in this manner.

#delete existing data off of db.

#dbFetch(dbSendQuery(con,"DELETE FROM detections WHERE procedure = 10 AND signal_code = 6 AND strength = 2 AND label = 1"))

#dbAppendTable('detections',)

#first, correct the fgs.
#the suspected incorrect fgs are:
#AW12_AU_CL01_files_441-516_bb_hg (done)
#BS12_AU_PM08_files_8101-8176_bb_hg (not actually missing any fg. same deal as below)
#AW15_AU_NM01_files_70-145_bb_hg (not actually missing any fg. The story here is that the boxes continue on while the effort stops.
#solutions are to either extend the gt using some of the next files from the original fn context, or just discard the labels)
#NOPP6_EST_20090403_files_All.csv (same deal!)

fgname = "NOPP6_EST_20090403_files_All"
sfiles_path ="//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Combined_sound_files/RW/No_whiten_decimate_by_16/NOPP6_EST_20090403_files_All_SFiles_and_durations.csv"

fg_fix = dbFetch(dbSendQuery(con,paste("SELECT bins.*,soundfiles.name,soundfiles.duration FROM soundfiles JOIN bins ON bins.soundfiles_id = soundfiles.id JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON effort.id = bins_effort.effort_id WHERE effort.name = '",fgname,"'",sep="")))
fg_fix_og = read.csv(sfiles_path)

fg_fix_og$bins.seg_start = as.numeric(substr(fg_fix_og$SFsh,nchar(fg_fix_og$SFsh)-6,nchar(fg_fix_og$SFsh)-4))/100*60*3
fg_fix_og$soundfiles.name = paste(substr(fg_fix_og$SFsh,1,nchar(fg_fix_og$SFsh)-19),
                                 substr(fg_fix_og$SFsh,nchar(fg_fix_og$SFsh)-16,nchar(fg_fix_og$SFsh)-7),"000.wav",sep="")

fg_fix_og$soundfiles.name = gsub("BS08a_","BSPM08-",fg_fix_og$soundfiles.name)

fg_fix_og$soundfiles.data_collection_id = 323
fg_fix_og$bins.type = 1

fg_fix_og_search = fg_fix_og[c("bins.seg_start","soundfiles.name","soundfiles.data_collection_id","bins.type")]

fg_fix_og_search$soundfiles.name = gsub("_","-",fg_fix_og_search$soundfiles.name)

fg_fix_og_lookup = table_dataset_lookup(con,"SELECT bins.*,soundfiles.name,soundfiles.duration,a,b,c,d FROM soundfiles JOIN bins ON bins.soundfiles_id = soundfiles.id",
                                       fg_fix_og_search,c("DOUBLE PRECISION","character varying","integer","integer"),return_anything = TRUE)

fg_fix_og_lookup$id = as.integer(fg_fix_og_lookup$id)

missing_bins = fg_fix_og_lookup$id[-which(fg_fix_og_lookup$id %in% as.integer(fg_fix$id))]

#get effort id

eff_id = dbFetch(dbSendQuery(con,paste("SELECT id FROM effort WHERE name = '",fgname,"'",sep="")))
newtab = data.frame(missing_bins,as.integer(eff_id$id))
colnames(newtab) = c("bins_id","effort_id")

dbAppendTable(con,'bins_effort',newtab)

#So, what to do next ?
#upload the old gt for AW12_AU_CL01_files_441-516_bb_hg and RW09_EA_01_files_1-75.txt (done!)
#upload the standard gt for the rest.
#delete those after which violate assumptions.
#done!

bb_path = "C:/Users/daniel.woodrich/Desktop/database/GroundTruth/BB/"

allbbs = dir(bb_path,full.names = TRUE)

#only upload those not already uploaded
allbbs = allbbs[2:7]

bb_all = list()

for(i in 1:length(allbbs)){

  bb_all[[i]] = read.csv(allbbs[i])

}

bb_all = do.call("rbind",bb_all)

bb_all$probs = NA
bb_all$comments = ""
bb_all$LastAnalyst = "DFW"

bb_all_db = detx_pgpamdb_det_rough_convert(con,bb_all,10,2)

dbAppendTable(con,'detections',bb_all_db)

bb_path = "//akc0ss-n086/NMML_CAEP_Acoustics/Detector/RavenBLEDscripts/Data/Selection tables/BB/RW09_EA_01Sum/RW09_EA_01_files_1-75.txt"
bb_fgname = "RW09_EA_UM01_files_1-75_bb_hg"




#try to recalculate i_neg. Use NOPP6_EST_20090328_files_All as a test.

dets = dbFetch(dbSendQuery(con,"SELECT * FROM detections JOIN bins_detections ON bins_detections.detections_id = detections.id JOIN bins ON bins.id = bins_detections.bins_id JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON bins_effort.effort_id = effort.id WHERE effort.name = 'NOPP6_EST_20090328_files_All'"))
FG = dbFetch(dbSendQuery(con,"SELECT bins.*,soundfiles.datetime FROM bins JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON bins_effort.effort_id = effort.id JOIN soundfiles ON bins.soundfiles_id = soundfiles.id WHERE effort.name = 'NOPP6_EST_20090328_files_All'"))

test = i_neg_interpolate(dets,FG,512,10,1,2)
test$comments = "test i_neg algorithm: testing data"

dbAppendTable(con,"detections",test)

dets_to_del = dbFetch(dbSendQuery(con,"SELECT id FROM detections WHERE comments = 'test i_neg algorithm: testing data'"))
dets_to_del = as.integer(dets_to_del$id)
table_delete(con,"detections",dets_to_del,hard_delete = TRUE)

#cool, looks good! Now, add these all back to db with a big loop. Can make functions to update one, and then all as needed.

fgname = "NOPP6_EST_20090328_files_All"
procedure = 10
signal_code = 1

i_neg_update(con,"NOPP6_EST_20090328_files_All",10,1,512)

#find all of the i_neg effort names which need to be updated.

i_neg_tab = dbFetch(dbSendQuery(con,"SELECT effort_procedures.*,effort.name FROM effort JOIN effort_procedures ON effort_procedures.effort_id = effort.id WHERE effort_procedures.effproc_assumption = 'i_neg'"))

#i_neg_tab = i_neg_tab[which(i_neg_tab$name!="NOPP6_EST_20090328_files_All"),]

#before doing loop below, find n's from i_neg procedures with comments, and change the procedure to protect them from deletion.

i_neg_0s = dbFetch(dbSendQuery(con,"SELECT * FROM detections WHERE procedure IN (10,11,12,13,17) AND label = 0"))

i_neg_0s = i_neg_0s[which(i_neg_0s$procedure!=10 & i_neg_0s$signal_code!=1),]

i_neg_0s$procedure = 0

i_neg_0s$comments = paste(i_neg_0s$comments,"#changed procedure by dfw to avoid deletion of 0 label data in i_neg effort.")

i_neg_0s$modified=NULL
i_neg_0s$analyst = NULL
i_neg_0s$status = NULL

table_update(con,'detections',i_neg_0s[,c("id","procedure","comments")])

#loop through this and add i_neg!

for(i in 45:nrow(i_neg_tab)){

  i_neg_update(con,i_neg_tab$name[i],as.integer(i_neg_tab$procedures_id[i]),as.integer(i_neg_tab$signal_code[i]))
#function(conn,fgname,procedure,signal_code,high_freq = NULL)
}


#add Jess's "g2" analysis to database.

#retrieved from email copy
jess_g2 = read.csv("C:/Users/daniel.woodrich/Downloads/AM-XBCS01_INSTINCT_review_final.csv")

jess_g2 = jess_g2[which(jess_g2$View=="Spectrogram 1"),]
#convert to db format
#jess_g2_db = detx_pgpamdb_det_rough_convert(con,jess_g2,19,2)

#would prefer not to do in this way since it's in raven format.
#check dbuddy

#anaylses_old = data_pull("SELECT * FROM analyses;")

#unfortunately, doesn't look like it was ever loaded in. so , probably convert from raven.
#also, can't retrieve it from running instinct params, probably too old at this point to be backwards compatible.
#so, do it old fashioned way. adapt above fxn.

fg= dbFetch(dbSendQuery(con,paste("SELECT bins.*,soundfiles.name,soundfiles.duration FROM soundfiles JOIN bins ON bins.soundfiles_id = soundfiles.id WHERE soundfiles.data_collection_id = 320 AND bins.type = 2",sep="")))

rav_og_data = jess_g2

rav_og_data$signal_code = 0
rav_og_data$signal_code[which(rav_og_data$label=='s')]=20
rav_og_data$signal_code[which(rav_og_data$label!='s')]=2

rav_og_data$procedure= 0
rav_og_data$procedure[which(rav_og_data$label=='s')]=20
rav_og_data$procedure[which(rav_og_data$label!='s')]=19

#copy this section, and make it also represent gs no's

rav_og_data_sp_copy = rav_og_data[which(rav_og_data$label=='s'),]

rav_og_data_sp_copy$label = 0
rav_og_data_sp_copy$procedure = 19
rav_og_data_sp_copy$signal_code = 2

rav_og_data$label[which(rav_og_data$label=='s')]='y'

label_lookup= lookup_from_match(con,'label_codes',unique(rav_og_data$label),'alias')
rav_og_data$label = label_lookup$id[match(rav_og_data$label,label_lookup$alias)]

rav_og_data = rbind(rav_og_data,rav_og_data_sp_copy)

#put fg in cons order

fg = fg[order(fg$name,fg$seg_start),]

fg$cons = c(0,cumsum(fg$seg_end-fg$seg_start)[1:length(cumsum(fg$seg_end-fg$seg_start))-1])

rav_og_data$dur = rav_og_data$End.Time..s.-rav_og_data$Begin.Time..s.

rav_og_data$start_file = fg$soundfiles_id[findInterval(rav_og_data$Begin.Time..s.,fg$cons)]
rav_og_data$end_file = fg$soundfiles_id[findInterval(rav_og_data$End.Time..s.,fg$cons)]

#no dets overlap files for this one.. all(rav_og_data$start_file==rav_og_data$end_file)

file_uniques = fg[which(fg$seg_start==0),]
file_uniques$cons = c(0,cumsum(file_uniques$duration)[1:nrow(file_uniques)-1])

rav_og_data$offset =rav_og_data$Begin.Time..s. - file_uniques$cons[findInterval(rav_og_data$Begin.Time..s.,file_uniques$cons)]

#think I have everything we need. load it in as dets.

template = dbFetch(dbSendQuery(con,'SELECT * FROM detections LIMIT 1'))

input = data.frame(rav_og_data$offset,rav_og_data$offset+rav_og_data$dur,rav_og_data$Low.Freq..Hz.,rav_og_data$High.Freq..Hz.,
                   rav_og_data$start_file,rav_og_data$end_file,rav_og_data$probs,rav_og_data$Comments,rav_og_data$procedure,rav_og_data$label,rav_og_data$signal_code,2)

colnames(input)=colnames(template)[2:(length(input)+1)]

dbAppendTable(con,"detections",input)

#going to upload the procedure where cole reviewed outputs of original lm detector at lower cutoff, use to compare with
#new model

#steps:
#load in component files (partial gt and fg)
#recalc raven file into single one based on cumdur
#use 'upload_from_oldold' with combined raven to convert and upload.

fg= dbFetch(dbSendQuery(con,paste("SELECT bins.*,soundfiles.name,soundfiles.duration FROM soundfiles JOIN bins ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id WHERE data_collection.name = 'BS13_AU_PM04' AND bins.type = 1",sep="")))

#rav_og_data = read.delim(bb_path)

#put fg in cons order

fg = fg[order(fg$name,fg$seg_start),]

fg$cons = c(0,cumsum(fg$seg_end-fg$seg_start)[1:length(cumsum(fg$seg_end-fg$seg_start))-1])

justfiles = fg[-which(duplicated(fg$name)),]
justfiles$cons =  c(0,cumsum(justfiles$duration[1:length(justfiles$duration)-1]))

path = "//akc0ss-n086/NMML_CAEP_Acoustics/Detector/LFmoan_project/ColeAnalysis/BS13_AU_04b p99 analyzed/BS13_AU_04b New threshold"
files = c("01BS13_AU_04b_files_All_LM_Model_Applied_probs_Cole_Reviewed.txt",
          "02BS13_AU_04b_files_All_LM_Model_Applied_probs_Cole_Reviewed.txt",
          "03BS13_AU_04b_files_All_LM_Model_Applied_probs_Cole_Reviewed.txt")
coldat_13_M4 = list()

for(i in 1:length(files)){

  coldat_13_M4[[i]] = read.delim(paste(path,files[i],sep="/"))
  coldat_13_M4[[i]] = coldat_13_M4[[i]][order(coldat_13_M4[[i]]$Begin.Time..s.),]
  coldat_13_M4[[i]]$chunk = i
}

coldat_13_M4 = do.call('rbind',coldat_13_M4)

coldat_13_M4$File_format = paste("AU-BSPM04",substr(coldat_13_M4$File,9,nchar(coldat_13_M4$File)),sep="")

#get the db sf durations
coldat_13_M4$file_dur = fg[match(coldat_13_M4$File_format,fg$name),"duration"]

#ajust the times. found them in SFiles in \\akc0ss-n086\NMML_CAEP_Acoustics\Detector\Combined_sound_files\No_whiten_decimate_by_128, copied by hand.
coldat_13_M4[which(coldat_13_M4$chunk==2),"Begin.Time..s."]= coldat_13_M4[which(coldat_13_M4$chunk==2),"Begin.Time..s."] + 4032000
coldat_13_M4[which(coldat_13_M4$chunk==3),"Begin.Time..s."]= coldat_13_M4[which(coldat_13_M4$chunk==3),"Begin.Time..s."] + 4032000+ 4032000

coldat_13_M4[which(coldat_13_M4$chunk==2),"End.Time..s."]= coldat_13_M4[which(coldat_13_M4$chunk==2),"End.Time..s."] + 4032000
coldat_13_M4[which(coldat_13_M4$chunk==3),"End.Time..s."]= coldat_13_M4[which(coldat_13_M4$chunk==3),"End.Time..s."] + 4032000+ 4032000

coldat_13_M4$dur = coldat_13_M4$End.Time..s.-coldat_13_M4$Begin.Time..s.

coldat_13_M4$start_file = justfiles$soundfiles_id[findInterval(coldat_13_M4$Begin.Time..s.,justfiles$cons)]
coldat_13_M4$end_file = justfiles$soundfiles_id[findInterval(coldat_13_M4$End.Time..s.,justfiles$cons)]

coldat_13_M4$Begin.Time..s. = coldat_13_M4$Begin.Time..s. - justfiles$cons[findInterval(coldat_13_M4$Begin.Time..s.,justfiles$cons)]
coldat_13_M4$End.Time..s. = coldat_13_M4$End.Time..s. - justfiles$cons[findInterval(coldat_13_M4$End.Time..s.,justfiles$cons)]

coldat_13_M4$tempid = 1:nrow(coldat_13_M4)

#have all the info I need to submit. But, I should put a temp id on these, so I can modify after with the post
#review changes (Dan files).

template = dbFetch(dbSendQuery(con,'SELECT * FROM detections LIMIT 1'))

input = data.frame(coldat_13_M4$Begin.Time..s.,coldat_13_M4$End.Time..s.,coldat_13_M4$Low.Freq..Hz.,coldat_13_M4$High.Freq..Hz.,
                   coldat_13_M4$start_file,coldat_13_M4$end_file,coldat_13_M4$LM.prob,coldat_13_M4$Comments,22,coldat_13_M4$Verification,3,2)

colnames(input)=colnames(template)[2:(length(input)+1)]

input$label[which(input$label=='n ')]="n"
input$label[which(input$label=='')]="uk"

lab_id = lookup_from_match(con,"label_codes",unique(input$label),"alias")
input$label = lab_id$id[match(input$label,lab_id$alias)]

#alright, can submit this.

dbAppendTable(con,'detections',input)

#alright, do the same for 'dan' detections.

files = c("01BS13_AU_04b_files_All_LM_Model_Applied_probs_Cole_Reviewed_Dan_Reviewed.txt",
          "02BS13_AU_04b_files_All_LM_Model_Applied_probs_Cole_Reviewed_Dan_Reviewed.txt",
          "03BS13_AU_04b_files_All_LM_Model_Applied_probs_Cole_Reviewed_Dan_Reviewed.txt")
dandat_13_M4 = list()

for(i in 1:length(files)){

  dandat_13_M4[[i]] = read.delim(paste(path,files[i],sep="/"))
  dandat_13_M4[[i]] = dandat_13_M4[[i]][order(dandat_13_M4[[i]]$Begin.Time..s.),]
  dandat_13_M4[[i]]$chunk = i
}

dandat_13_M4 = do.call('rbind',dandat_13_M4)

dandat_13_M4$File_format = paste("AU-BSPM04",substr(dandat_13_M4$File,9,nchar(dandat_13_M4$File)),sep="")

#get the db sf durations
dandat_13_M4$file_dur = fg[match(dandat_13_M4$File_format,fg$name),"duration"]

#ajust the times. found them in SFiles in \\akc0ss-n086\NMML_CAEP_Acoustics\Detector\Combined_sound_files\No_whiten_decimate_by_128, copied by hand.
dandat_13_M4[which(dandat_13_M4$chunk==2),"Begin.Time..s."]= dandat_13_M4[which(dandat_13_M4$chunk==2),"Begin.Time..s."] + 4032000
dandat_13_M4[which(dandat_13_M4$chunk==3),"Begin.Time..s."]= dandat_13_M4[which(dandat_13_M4$chunk==3),"Begin.Time..s."] + 4032000+ 4032000

dandat_13_M4[which(dandat_13_M4$chunk==2),"End.Time..s."]= dandat_13_M4[which(dandat_13_M4$chunk==2),"End.Time..s."] + 4032000
dandat_13_M4[which(dandat_13_M4$chunk==3),"End.Time..s."]= dandat_13_M4[which(dandat_13_M4$chunk==3),"End.Time..s."] + 4032000+ 4032000

dandat_13_M4$dur = dandat_13_M4$End.Time..s.-dandat_13_M4$Begin.Time..s.

dandat_13_M4$start_file = justfiles$soundfiles_id[findInterval(dandat_13_M4$Begin.Time..s.,justfiles$cons)]
dandat_13_M4$end_file = justfiles$soundfiles_id[findInterval(dandat_13_M4$End.Time..s.,justfiles$cons)]

dandat_13_M4$Begin.Time..s. = dandat_13_M4$Begin.Time..s. - justfiles$cons[findInterval(dandat_13_M4$Begin.Time..s.,justfiles$cons)]
dandat_13_M4$End.Time..s. = dandat_13_M4$End.Time..s. - justfiles$cons[findInterval(dandat_13_M4$End.Time..s.,justfiles$cons)]

onlydiff = dandat_13_M4[-which(paste(dandat_13_M4$Begin.Time..s.,dandat_13_M4$End.Time..s.,dandat_13_M4$start_file,dandat_13_M4$Verification) %in%
                                 paste(coldat_13_M4$Begin.Time..s.,coldat_13_M4$End.Time..s.,coldat_13_M4$start_file,coldat_13_M4$Verification)),]

onlydiff$procedure = 22

onlydiff = data.frame(onlydiff$Begin.Time..s.,onlydiff$End.Time..s.,onlydiff$Low.Freq..Hz.,onlydiff$High.Freq..Hz.,
                      onlydiff$start_file,onlydiff$end_file,onlydiff$LM.prob,onlydiff$Comments,22,onlydiff$Verification,3,2)

colnames(onlydiff)=colnames(template)[2:(length(onlydiff)+1)]
#for onlydiff, pull out of db to get ids

onlydiff$start_file = as.integer(onlydiff$start_file)
onlydiff$end_file = as.integer(onlydiff$end_file)

w_ids = table_dataset_lookup(con,"SELECT * FROM detections",onlydiff[,c(1,2,5,6,7,9)],
                             c("DOUBLE PRECISION","DOUBLE PRECISION","integer","integer","DOUBLE PRECISION","integer"))

w_ids = w_ids[order(w_ids$start_time),]
onlydiff = onlydiff[order(onlydiff$start_time),]

#now they match= cbind ids, and update verification.

ds = data.frame(w_ids$id,onlydiff$label)

colnames(ds)=c("id","label")

ds$label = lab_id$id[match(ds$label,lab_id$alias)]

table_update(con,'detections',ds)

#done!

#how to compare stats? Don't at first- first visaulize model (upload detections, and then visualize) to see if it is
#worth comparing. Then, pull all detections and come up with a quick script (midpoint) to compare performance.


#now, upload results from full detector.

#here is the csv extracted from a pipeline- don't quite want to build out full procedure yet for verification.

dets = read.csv("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Datasets_transfer/DETx.csv.gz")

template = dbFetch(dbSendQuery(con,"SELECT * FROM detections LIMIT 1"))

colnames(dets)[1:7]=colnames(template)[2:8]

#BS13_AU_PM04
dets$FGID=NULL
dets$splits=NULL

#format to db format

file_lookup = lookup_from_match(con,"soundfiles",unique(c(dets$start_file,dets$end_file)),"name")

dets$start_file = file_lookup$id[match(dets$start_file,file_lookup$name)]
dets$end_file = file_lookup$id[match(dets$end_file,file_lookup$name)]

dets$comments = ""
dets$procedure = 21
dets$label = 21
dets$signal_code = 3
dets$strength = 1

#dbAppendTable(con,'detections',dets)
#now to submit the negatives, slightly different needs here


fgquery = gsub("[\r\n]", "",paste("SELECT soundfiles.name,'','',duration,data_collection.name,bins.seg_start,bins.seg_end-bins.seg_start FROM soundfiles JOIN data_collection ON data_collection.id
                                  = soundfiles.data_collection_id JOIN bins ON bins.soundfiles_id = soundfiles.id WHERE bins.type = 1 AND data_collection.name = 'BS13_AU_PM04'",sep=""))

allfg = dbFetch(dbSendQuery(con,fgquery))

colnames(allfg) =c("FileName" ,"FullPath" ,"StartTime","Duration" ,"Deployment","SegStart"   ,"SegDur")
dets_og = read.csv("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Datasets_transfer/DETx.csv.gz")

dets_og$visible_hz = ""
dets_og$label='py'
dets_og$uk= ""
dets_og$SignalCode = 'LM'
dets_og$type= ""
dets_og$uk2 = ""
dets_og$analyst="DFW"

dets_og$splits = NULL
dets_og$FGID=NULL

alldata_negs = bin_negatives(dets_og,allfg,"LOW","DFW")

colnames(alldata_negs)[1:7]=colnames(template)[2:8]

file_lookup = lookup_from_match(con,"soundfiles",unique(c(alldata_negs$start_file,alldata_negs$end_file)),"name")

alldata_negs$start_file = file_lookup$id[match(alldata_negs$start_file,file_lookup$name)]
alldata_negs$end_file = file_lookup$id[match(alldata_negs$end_file,file_lookup$name)]


alldata_negs$visible_hz = NULL
alldata_negs$uk = NULL
alldata_negs$uk2 = NULL
alldata_negs$SignalCode = NULL
alldata_negs$type = NULL
alldata_negs$analyst = NULL


alldata_negs$comments = ""
alldata_negs$procedure = 21
alldata_negs$label = 20
alldata_negs$signal_code = 3
alldata_negs$strength = 1

dbAppendTable(con,'detections',alldata_negs)
dbSendQuery(con,"DELETE FROM detections WHERE modified = '2023-02-14 00:00:48.030469+00'")

#load in a mooring which didn't make it in-

#give bin negatives a try with db format.
#load in the fg and 'dets' for

#see if dets got loaded in:

test = dbFetch(dbSendQuery(con,"SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id WHERE data_collection.name = 'AW15_AU_PH01' AND detections.procedure = 5"))

#doesn't look like it

fg = dbFetch(dbSendQuery(con,"SELECT bins.*,duration FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id WHERE data_collection.name = 'AW15_AU_PH01' AND bins.type = 1"))
dets = dbFetch(dbSendQuery(con,"SELECT * FROM detections LIMIT 0"))

dets$start_file = as.integer(dets$start_file)
dets$end_file = as.integer(dets$end_file)

fg$soundfiles_id = as.integer(fg$soundfiles_id)

test= bin_negatives(dets,fg,"LOW",analyst = NULL,procedure = 5,signal_code = 3)

dbAppendTable(con,"detections",test)

test = bin_label_explore(con,"fw")
test2 = bin_label_explore(con,"lm")

#looking into possibility of bugged data. I know that BS15_AU_PM04 was bugged for fin whales, so also checking it out for
#low moans.

#test out capability to assess lm detector deployment for 1. completeness 2. correctness.
#for this one, safe to assume that only full moorings are run.
#let's start out with a query # of bins in mooring where a 1 or 20 are present, divided by total bins.

bins_w_analysis = "SELECT COUNT(*),subquery.name FROM (SELECT DISTINCT ON (bins.id) COUNT(*),data_collection.name FROM detections JOIN bins_detections ON bins_detections.detections_id = detections.id JOIN bins ON bins.id = bins_detections.bins_id
 JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE bins.type = 1 AND detections.label IN (1,20) AND detections.procedure = 5 GROUP BY bins.id,data_collection.name) AS subquery GROUP BY subquery.name"

bins_w_analysis_out = dbGet(bins_w_analysis)

allbins = "SELECT COUNT(*),data_collection.name FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id =
soundfiles.data_collection_id WHERE bins.type = 1 GROUP BY data_collection.name"

bins_all = dbGet(allbins)

comp = merge(bins_w_analysis_out,bins_all,by="name",all.y = TRUE)

comp$perc = comp$count.x/comp$count.y

#look at the bins which are in disagreement.

bins_w_analysis_disagree = "SELECT COUNT(*),subquery.name FROM (SELECT DISTINCT ON (bins.id) COUNT(*),data_collection.name FROM detections JOIN bins_detections ON bins_detections.detections_id = detections.id JOIN bins ON bins.id = bins_detections.bins_id
 JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE bins.type = 1 AND detections.label IN (1,20) AND detections.procedure = 5 GROUP BY bins.id,data_collection.name) AS subquery WHERE subquery.count = 2 GROUP BY subquery.name"

#bins_w_analysis_disagree_out = dbGet(bins_w_analysis)


#find the bins in BS09_AU_PM02-a which don't have labels for 5

missing_5 = "SELECT * FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
 WHERE data_collection.name = 'BS09_AU_PM02-a' AND bins.type =1 AND bins.id NOT IN (SELECT bins.id FROM bins JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN detections ON bins_detections.detections_id = detections.id
 JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE bins.type =1 AND data_collection.name = 'BS09_AU_PM02-a'
 AND detections.procedure =5 AND label IN (1,20))"

missing_5_out = dbGet(missing_5)

#looks like the bug is that I incorrectly filled in 0 and 600 as the soundfile size when I uploaded LM bin assumptions. oops!
#easiest step is probably to delete and resubmit.

#There are some other issues, related to soundfile naming- in cases where effort extended between different moorings, it sometimes
#referred to the wrong deployment. Make sure before deleting these, that the data are also present in the correct mooring.

#how to modify detections so they represent the right deployment?

#first, identify the problem moorings. Based on the stats, looks like
#1. AW12_AU_WT01
#2. BS08_AU_PM05
#3. AL19_AU_BS09
#4. CX12_AU_WT02

#steps:
#1. pull submitted data (0 or 1)

lm_data = "SELECT detections.*,data_collection.id,data_collection.name FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE signal_code = 3 AND procedure = 5 AND status = 1 AND label IN (0,1)"

lm_data_out = dbGet(lm_data)

#use above for inventory of moorings.

#2. recalculate negative bins
#3. delete lm procedure 5 (label 20)
#4. resubmit.

#alternate way forward-
#1. pull all detections with 0 or 1 (along with mooring name)
#2. for those which do not go with a mooring which was completed, locate their matching soundfile name and update the detection
#to represent where it should be

lm_wrong_moor = lm_data_out[which(lm_data_out$name %in% c("AW12_AU_WT01","BS08_AU_PM05","AL19_AU_BS09","CX12_AU_WT02")),]

dup_sfs = dbGet("SELECT * from (SELECT soundfiles.id,soundfiles.name,data_collection.name,
  COUNT(*) OVER(PARTITION BY soundfiles.name ORDER BY soundfiles.id asc rows BETWEEN unbounded preceding AND unbounded following) AS Row
  FROM soundfiles JOIN data_collection ON soundfiles.data_collection_id = data_collection.id)
  AS dups
  WHERE dups.Row > 1")

#test that the issues are all coming from duplicated file names:

comp[which(comp$perc <1),"name"][which(comp[which(comp$perc <1),"name"] %in% unique(dup_sfs$name..3))]

#looks like all the above, except BS09_AU_PM02-a , are cases where sf names are duplicated. To fix this one,
#just need to submit remaining bins as 20s.

fix_M2009 = data.frame(missing_5_out$seg_start,missing_5_out$seg_end,0,64,missing_5_out$soundfiles_id,missing_5_out$soundfiles_id,
                       NA,"",5,20,3,2)

cols =  dbGet("SELECT * FROM detections LIMIT 0")

colnames(fix_M2009)=colnames(cols)[2:13]
#dbAppendTable(con,'detections',fix_M2009)

#did notice that some 20s are high_freq 512 in procedure 5- should be 64. change.

change_data =dbGet("SELECT * FROM detections WHERE procedure = 5 AND label = 20 AND high_freq = 512")

#fix these data, hard delete old ones, resubmit.
new_data = change_data[,2:16]
new_data$high_freq = 64

#table_delete(con,'detections',change_data$id,hard_delete = TRUE)
#dbAppendTable(con,'detections',new_data)

#fixed!

#determine which of the remaining should have been run, and which shouldn't have been run.

is_run = c(comp[which(comp$perc <1),"name"])
is_run_bool = c(F,F,)

#now, can proceed assume affect moorings are due to sf duplicate issue.
#in addition to missing data being possible, it may be true that there is double counted data as well when
#both of the moorings have been run.
#for instance, AL16_AU_UM01 is missing some data, but AL17_AU_UM01 is not- it may have duplicated counts.
#this is true for (one missing data in brackets, duplicated perhaps not in brackets) [AL16_AU_UM01]-AL17_AU_UM01,IP16_AU_CH01-[IP17_AU_CH01], [CX14_AU_IC03]-CX13_AU_IC03
#in order, confirmed problem sfs: AU-ALUM01-170508-000000.wav,AU-IPCH01-171006-000000.wav,AU-CXIC03-140926-100000.wav

#doesn't look like it based on this
test= dbGet("SELECT detections.*,soundfiles.name,data_collection.name FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE procedure = 5 AND soundfiles.name IN ('AU-ALUM01-170508-000000.wav','AU-IPCH01-171006-000000.wav','AU-CXIC03-140926-100000.wav')")

#so need to handle this in chunks: 1, pull out the duplicated data and reassign, and 2, pull out the misassigned data and reassign.
#need to make sure data are indeed duplicated. Test out each

#get all of the detections from the moorings which both were run, to see if there are any duplicates.

dups = dbGet(paste("SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id WHERE procedure = 5 AND soundfiles.name IN ('",paste(dup_sfs$name[which(dup_sfs$name..3 %in% c('AW13_AU_WT01','CX12_AU_WT02'))],sep="",collapse="','"),"')",sep=""))
#doesn't appear as though any detections are duplicated.

#test = dbGet("SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id
#              JOIN data_collection ON soundfiles.data_collection_id = data_collection.id WHERE date_trunc('day', datetime) = '2017-05-09' AND procedure = 5 AND data_collection.location_code ='UM01'")

#test = dbGet("SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id
#              JOIN data_collection ON soundfiles.data_collection_id = data_collection.id WHERE date_trunc('day', datetime) = '2014-09-26' AND procedure = 5 AND data_collection.location_code ='IC03'")



#looking at this single day- it appears that there is a mix of data from both sources, although it is non-overlapping.

#so, what to do with this information. One thing I could do is pull out all of the dets from these, validate that they are in correct
#mooring, and then resubmit negatives.

View(test[which(test$label!=20),c("probability","name","name..24")])

#for AU-ALUM01-170508-000000.wav: all detections in overlapping sections should be from 1st mooring (no overlap seen in detector outputs)
#So what to do:
#1. update all of these so that they are in 16 mooring.
#find any 99 bins in 16 or 17: add bin negatives to them.

#updatedets = dbGet("SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id
#              JOIN data_collection ON soundfiles.data_collection_id = data_collection.id WHERE date_trunc('day', datetime) >= '2017-05-08' AND date_trunc('day', datetime) < '2017-07-20' AND procedure = 5 AND label !=20 AND data_collection.location_code ='UM01'")


#2014-09-26

#ignore those which have the correct mooring name
updatedets = updatedets[-which(updatedets$name..24=="AL16_AU_UM01"),]

#load the correct file names, using the ids
sf_ids = dbGet(paste("SELECT id,name FROM soundfiles WHERE id IN (",paste(unique(c(updatedets$start_file,updatedets$end_file)),sep="",collapse = ","),")",sep=""))

sf_ids2 = dbGet(paste("SELECT id,name FROM soundfiles WHERE name IN ('",paste(sf_ids$name,sep="",collapse = "','"),"')",sep=""))

sf_ids2$id = as.integer(sf_ids2$id)

sf_ids2_a = sf_ids2[which(sf_ids2$id<1000000),]
sf_ids2_b = sf_ids2[which(sf_ids2$id>1000000),]

sf_ids_3 = merge(sf_ids2_a,sf_ids2_b,by="name")

#nice, now do replacement

updatedets$start_file = as.integer(updatedets$start_file)
updatedets$end_file = as.integer(updatedets$end_file)

updatedets$start_file = sf_ids_3$id.y[match(updatedets$start_file,sf_ids_3$id.x)]
updatedets$end_file[which(updatedets$end_file %in% sf_ids_3$id.x)] = sf_ids_3$id.y[match(updatedets$end_file[which(updatedets$end_file %in% sf_ids_3$id.x)],sf_ids_3$id.x)]

#submit updated detections, them, hard delete old detections.

updatedets_UM01 = data.frame(updatedets$start_time,updatedets$end_time,updatedets$low_freq,updatedets$high_freq,updatedets$start_file,updatedets$end_file,
                             updatedets$probability,updatedets$comments,updatedets$procedure,updatedets$label,updatedets$signal_code,
                             updatedets$strength,updatedets$modified,updatedets$analyst)
colnames(updatedets_UM01) =colnames(cols)[2:15]

#insert

#dbAppendTable(con,'detections',updatedets_UM01)

#now delete old ones
#table_delete(con,'detections',updatedets$id,hard_delete = TRUE)

#cool, now if it still has missing, just upload the missing bins as negatives.

missing_5 = "SELECT * FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
 WHERE data_collection.name = 'AL16_AU_UM01' AND bins.type =1 AND bins.id NOT IN (SELECT bins.id FROM bins JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN detections ON bins_detections.detections_id = detections.id
 JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE bins.type =1 AND data_collection.name = 'AL16_AU_UM01'
 AND detections.procedure =5 AND label IN (1,20))"

missing_5_out = dbGet(missing_5)

#all files are 600 seconds, so create the dets with that assumption.

prot_negs_AL16_AU_UM01 = data.frame(0,600,0,64,unique(missing_5_out$soundfiles_id),unique(missing_5_out$soundfiles_id),
                                    NA,"",5,20,3,2)
colnames(prot_negs_AL16_AU_UM01) = colnames(cols)[2:13]

#dbAppendTable(con,'detections',prot_negs_AL16_AU_UM01)

#alright, repeat the above steps for the other moorings.

updatedets = dbGet("SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id
              JOIN data_collection ON soundfiles.data_collection_id = data_collection.id WHERE date_trunc('day', datetime) = '2014-09-26' AND procedure = 5 AND label !=20 AND data_collection.location_code ='IC03'")

#for ic3 - 14, there are no detections which need to be moved. So, I can just fill in the remaining bins.

missing_5 = "SELECT * FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
 WHERE data_collection.name = 'CX14_AU_IC03' AND bins.type =1 AND bins.id NOT IN (SELECT bins.id FROM bins JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN detections ON bins_detections.detections_id = detections.id
 JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE bins.type =1 AND data_collection.name = 'CX14_AU_IC03'
 AND detections.procedure =5 AND label IN (1,20))"

missing_5_out = dbGet(missing_5)

prot_negs_CX14_AU_IC03 = data.frame(0,600,0,64,unique(missing_5_out$soundfiles_id),unique(missing_5_out$soundfiles_id),
                                    NA,"",5,20,3,2)
colnames(prot_negs_CX14_AU_IC03) = colnames(cols)[2:13]

#dbAppendTable(con,'detections',prot_negs_CX14_AU_IC03)

#in cases where both moorings have been run, how do I know which is the correct mooring for the data. Can I?
#I could spot check detections against the detector outputs to see if it is correct.
#In one case, a detection has a start file in one mooring and an end file in the other... :/


#CX13_AU_WT02: looks like a case where I simply didn't run the detector on the whole data, so what are there are correct.

#now do IP17_AU_CH01

updatedets = dbGet("SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id
              JOIN data_collection ON soundfiles.data_collection_id = data_collection.id WHERE date_trunc('day', datetime) = '2017-10-06' AND procedure = 5 AND label !=20 AND data_collection.location_code ='CH01'")

#these both were detected in 17, but are currently in 16, so need to move them.

sf_ids = dbGet(paste("SELECT id,name FROM soundfiles WHERE id IN (",paste(unique(c(updatedets$start_file,updatedets$end_file)),sep="",collapse = ","),")",sep=""))
sf_ids2 = dbGet(paste("SELECT id,name FROM soundfiles WHERE name IN ('",paste(sf_ids$name,sep="",collapse = "','"),"')",sep=""))
sf_ids2=sf_ids2[order(sf_ids2$name),]

updatedets$start_file=c(2098995,2099016)
updatedets$end_file=c(2098995,2099016)

updatedets_IP17_AU_CH01 = data.frame(updatedets$start_time,updatedets$end_time,updatedets$low_freq,updatedets$high_freq,updatedets$start_file,updatedets$end_file,
                             updatedets$probability,updatedets$comments,updatedets$procedure,updatedets$label,updatedets$signal_code,
                             updatedets$strength,updatedets$modified,updatedets$analyst)
colnames(updatedets_IP17_AU_CH01) =colnames(cols)[2:15]

#dbAppendTable(con,'detections',updatedets_IP17_AU_CH01)

#now delete old ones
#table_delete(con,'detections',updatedets$id,hard_delete = TRUE)

missing_5 = "SELECT * FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
 WHERE data_collection.name = 'IP17_AU_CH01' AND bins.type =1 AND bins.id NOT IN (SELECT bins.id FROM bins JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN detections ON bins_detections.detections_id = detections.id
 JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE bins.type =1 AND data_collection.name = 'IP17_AU_CH01'
 AND detections.procedure =5 AND label IN (1,20))"

missing_5_out = dbGet(missing_5)

#hmm, didn't remember if I checked that sfs are all duration 600. Not worth going back, at the end do a single check
#and correct for any detections where end time > duration of file.
prot_negs_IP17_AU_CH01 = data.frame(0,600,0,64,unique(missing_5_out$soundfiles_id),unique(missing_5_out$soundfiles_id),
                                    NA,"",5,20,3,2)
colnames(prot_negs_IP17_AU_CH01) = colnames(cols)[2:13]

#dbAppendTable(con,'detections',prot_negs_IP17_AU_CH01)

#now do CX12_AU_WT02

updatedets = dbGet("SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id
              JOIN data_collection ON soundfiles.data_collection_id = data_collection.id WHERE date_trunc('day', datetime) >= '2013-08-30' AND datetime <= '2013-10-02 14:10:00' AND procedure = 5 AND label !=20 AND data_collection.location_code ='WT02'")

#these are all correct, don't need to move them. So, only thing I need to do is add the missing negatives.

missing_5 = "SELECT * FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
 WHERE data_collection.name = 'CX12_AU_WT02' AND bins.type =1 AND bins.id NOT IN (SELECT bins.id FROM bins JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN detections ON bins_detections.detections_id = detections.id
 JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE bins.type =1 AND data_collection.name = 'CX12_AU_WT02'
 AND detections.procedure =5 AND label IN (1,20))"

missing_5_out = dbGet(missing_5)

prot_negs_CX12_AU_WT02 = data.frame(0,600,0,64,unique(missing_5_out$soundfiles_id),unique(missing_5_out$soundfiles_id),
                                    NA,"",5,20,3,2)
colnames(prot_negs_CX12_AU_WT02) = colnames(cols)[2:13]

#dbAppendTable(con,'detections',prot_negs_CX12_AU_WT02)

#last, need to take the remaining 3 moorings and switch all dets over. pseudo:
#1.query all dets on not run moorings
#2.query all sfs that match sf names of these, (sf2 tab)
#3.remove from sf2 all matches to sf ids in table
#4.match id to remaining ids in sf2 tab using name in detections
#resubmit (probably, reinsert and then hard delete)

wrong_dets= dbGet("SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id
                  JOIN data_collection ON soundfiles.data_collection_id = data_collection.id
                  WHERE procedure = 5 AND data_collection.name IN ('AW12_AU_WT01','BS08_AU_PM05','AL19_AU_BS09')")

sf_ids = dbGet(paste("SELECT id,name FROM soundfiles WHERE id IN (",paste(unique(c(wrong_dets$start_file,wrong_dets$end_file)),sep="",collapse = ","),")",sep=""))

sf_ids$id = as.integer(sf_ids$id)
wrong_dets$start_file=as.integer(wrong_dets$start_file)
wrong_dets$end_file=as.integer(wrong_dets$end_file)

wrong_dets$start_file_name = sf_ids$name[match(wrong_dets$start_file,sf_ids$id)]
wrong_dets$end_file_name = sf_ids$name[match(wrong_dets$end_file,sf_ids$id)]

sf_ids2 = dbGet(paste("SELECT id,name FROM soundfiles WHERE name IN ('",paste(sf_ids$name,sep="",collapse = "','"),"')",sep=""))

sf_ids2$id = as.integer(sf_ids2$id)

sf_ids3 = sf_ids2[-which(sf_ids2$id %in% unique(c(wrong_dets$start_file,wrong_dets$end_file))),]

wrong_dets$start_file2 = sf_ids3$id[match(wrong_dets$start_file_name,sf_ids3$name)]
wrong_dets$end_file2 = sf_ids3$id[match(wrong_dets$start_file_name,sf_ids3$name)]

#insert detections into correct mooring:

updatedets_allwrong = data.frame(wrong_dets$start_time,wrong_dets$end_time,wrong_dets$low_freq,wrong_dets$high_freq,wrong_dets$start_file2,wrong_dets$end_file2,
                                 wrong_dets$probability,"",wrong_dets$procedure,wrong_dets$label,wrong_dets$signal_code,
                                 wrong_dets$strength,wrong_dets$modified,wrong_dets$analyst)
colnames(updatedets_allwrong) =colnames(cols)[2:15]

#dbAppendTable(con,'detections',updatedets_allwrong)

#delete old data
table_delete(con,'detections',wrong_dets$id,hard_delete = TRUE)

#done! However, I should think about if there is potential for detections to have been double submitted, and continue to look out for it.
#one final thing- want to now take a look at dets which exceed their sf size. But, do that next time.
#last time, a lot of lm 5 procedure offended this.

bug_dets = dbGet("SELECT * FROM detections JOIN soundfiles ON detections.end_file = soundfiles.id
                 WHERE detections.status = 1 AND detections.end_time > soundfiles.duration")

#get counts of all vers of detections- see if errs are more related to updates or original upload. looks like more related to og upload
dbGet(paste("SELECT COUNT(*) FROM detections WHERE original_id IN (",paste(bug_dets$original_id,sep="",collapse=","),")",sep=""))

#still some bugs. Split this into two camps- rounding errors (or mistaken duration) and miscalculated soundfiles.
#for rounding errors, starttime should be within the duration, for miscalculated soundfiles, it should be outside of it.

round_err = dbGet("SELECT * FROM detections JOIN soundfiles ON detections.end_file = soundfiles.id
                 WHERE detections.end_time > soundfiles.duration AND detections.start_file = detections.end_file AND detections.start_time < soundfiles.duration")

dbGet(paste("SELECT COUNT(*) FROM detections WHERE original_id IN (",paste(round_err$original_id,sep="",collapse=","),")",sep=""))

#the plan: just truncate these to the end of the soundfile duration, then submit, then delete originals by original_id.

round_err$end_time2 = round_err$end_time
round_err$end_time = round_err$duration

round_errdets = round_err[,c(2:18),] #all original values minus id

#looks good- resubmit.

#dbAppendTable(con,'detections',round_errdets)

#wait, that does not work because now if I delete by original id I delete them all. Better, but riskier, to hard delete
#all based on original id, and then resubmit just the dataset in memory.

#this function is kindof sus. I should make it a lot safer
#1. more atomic
#2. use the information on sequence to predict the new ids, and then use those specific ids to delete.
#table_delete(con,'detections',as.integer(round_err$id),hard_delete = TRUE)

#fixed! I think before I go further, I should really try to fix table_delete so I can actually use it predictably without
#breaking stuff.

#experiment with table_delete2 on a dummy table in a test db to see if it works.

con2 = pamdbConnect('test',keyscript,clientkey,clientcert)

dbFetch(dbSendQuery(con2,"SELECT * FROM increment_test_table"))

dbFetch(dbSendQuery(con2,"SELECT currval('increment_test_table_id_seq')"))

#alright, fill in some data.
testdata = data.frame(c(1,2,3,4,5,6))
names(testdata) = 'data'


testdata2 = data.frame(7,7)
colnames(testdata2) = c('id','data')

#dbAppendTable(con2,'increment_test_table',testdata)

#table_delete2(con2,'increment_test_table',c(3,4,5))

#dbAppendTable(con2,'increment_test_table',testdata2)

testdata3 = data.frame(8)
names(testdata3) = 'data'

dbAppendTable(con2,'increment_test_table',testdata3)

#test out new table_delete with dummy data. First, load in some dummy data.

template = dbGet("SELECT * FROM detections LIMIT 3")

#going to make it as simple to locate as possible

template$id = NULL
template$start_time = c(1,2,3)
template$end_time = c(2,3,4)
template$low_freq = c(100,100,100)
template$high_freq = c(200,200,200)
template$start_file=1
template$end_file=1
template$probability=NA
template$comments="dummy data for function testing- delete if encountered"
template$procedure=0
template$label=99
template$signal_code=1
template$strength=1

#submit template as new data.

dbAppendTable(con,'detections',template)

dbGet("SELECT * FROM detections WHERE start_file = 1")

#try out new table delete on these. do it line by line manually at first just in case.

ids = c(35624874,35624875,35624876)

#Seems to work and make sense. Now, try it with the new function.
data_save =dbGet("SELECT * FROM detections WHERE original_id IN (156651,156652,156653)")

#add back in template:

#dbAppendTable(con,'detections',template)

#cool, it works.
#table_delete(con,'detections',c(35624880,35624881,35624882),hard_delete = TRUE)

#alright, now, back to the LM data cleaning. Where was I?
#need to look at data where the soundfile appears to be miscalculated. Not sure if theres a way to do this other than
#a lot of individual queries in a loop.


bug_dets = dbGet("SELECT * FROM detections JOIN soundfiles ON detections.end_file = soundfiles.id
                 WHERE detections.status = 1 AND detections.end_time > soundfiles.duration")

#some remaining cases we have:
#both start and end file exceed duration (means it is probably next file- but hard to validate this.)
#end file is a seperate file, but duration of that file was miscalculated.

#one case is that there are some 20 dets which exceed length of soundfile, completely. These can be safely deleted.

bug_dets_no_exist = bug_dets[which(bug_dets$label==20),]

#table_delete(con,'detections',as.integer(bug_dets_no_exist$id),hard_delete = TRUE)

#refresh bug_dets, and work on other cases.
#the case where the file is different but end file dur exceeds is pretty safe to assume that the end duration
#is not correctly relevant to the start of the new file. Remove any stragler cases and change these in bulk.

bug_dets_incor_end = bug_dets[which(bug_dets$start_file!=bug_dets$end_file),]

bug_dets$id = as.integer(bug_dets$id)

outlier_cases = bug_dets_incor_end[which(bug_dets_incor_end$id %in% c(31246446,31246439,31246457,31246442,30497935)),]

bug_dets_incor_end = bug_dets_incor_end[-which(bug_dets_incor_end$id %in% c(31246446,31246439,31246457,31246442,30497935)),]

bug_dets_incor_end$subtract_by = bug_dets_incor_end$duration

bug_dets_incor_end$subtract_by[which(bug_dets_incor_end$subtract_by %in% c(59,60))]=300

#subtract, then add these back to db. Finally, delete old data.

bug_dets_incor_end$end_time = bug_dets_incor_end$end_time-bug_dets_incor_end$subtract_by

fix_incor_end = bug_dets_incor_end[,c(1:18)]

#dbAppendTable(con,'detections',fix_incor_end[,c(2:length(fix_incor_end))])

#table_delete(con,'detections',as.integer(fix_incor_end$id),hard_delete = TRUE)

#alright, now, keep working on cases. Let's take a look at the 'outlier' cases.
#one is easy, the rest are not.

eZ1 = outlier_cases[which(as.integer(outlier_cases$data_collection_id)==151),]

eZ1$end_file =27234

#dbAppendTable(con,'detections',eZ1[,c(2:18)])

#table_delete(con,'detections',as.integer(eZ1$id),hard_delete = TRUE)
#to evalute the rest- if they are correctly positioned on the data, need to adjust file and duration. If they are incorrectly
#positioned, should adjust to make it accurate to file and then take a look at duration again.

#so, next step is to see if I can make a query for these. but, currently locked out of the VM

#hard to say what to do. For M2 2009, data are not matching up with the labels. Continue to investigate?
#checking- is any data from M2 2009 correct?


#in the meantime- I can use the output of the original query for mooring completeness to assess progress for a given

#what about - is there a query which can provide a graphical look at moorings which have or have not been run for a procedure?


procedure_prog = function(conn,procedure_ids){

  bins_w_analysis = paste("SELECT COUNT(*),subquery.name FROM (SELECT DISTINCT ON (bins.id) COUNT(*),data_collection.name FROM detections JOIN bins_detections ON bins_detections.detections_id = detections.id JOIN bins ON bins.id = bins_detections.bins_id
 JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id WHERE bins.type = 1 AND detections.label IN (1,20) AND detections.procedure IN (",paste(procedure_ids,collapse=",",sep=""),") GROUP BY bins.id,data_collection.name) AS subquery GROUP BY subquery.name",sep='')

  bins_w_analysis_out = dbFetch(dbSendQuery(conn,bins_w_analysis))

  allbins = "SELECT COUNT(*),data_collection.name,MIN(soundfiles.datetime),MAX(soundfiles.datetime),location_code,latitude FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id =
soundfiles.data_collection_id WHERE bins.type = 1 GROUP BY data_collection.name,location_code,latitude"

  bins_all = dbFetch(dbSendQuery(conn,allbins))

  comp = merge(bins_w_analysis_out,bins_all,by="name",all.y = TRUE)

  comp$perc = comp$count.x/comp$count.y

  latlookup = aggregate(comp,list(comp$location_code),function(x) mean(x,na.rm=TRUE))

  comp$location_code =factor(comp$location_code,level = rev(latlookup$Group.1[order(latlookup$latitude)]))


  comp$interp = "unanalyzed"
  comp$interp[which(comp$perc==1)]= "analyzed"
  comp$interp[which(comp$perc!=1)]= "partially analyzed"
  #add a 'ymin' column to comp, depending on if it overlaps within same

  #really, should return plot of moorings run and moorings to go
  #ymin = ymin, ymax = ymin + 1,

  comp$ymin = 0

  if(any(is.na(comp$location_code))){

    comp=comp[-which(is.na(comp$location_code)),]

  }

  out = ggplot(comp, aes(xmin = min, xmax = max,ymin = ymin, ymax = ymin+1, fill = factor(interp))) + geom_rect(color="black") +
    facet_grid(location_code~., switch = "y")+  #opts(axis.text.y = theme_blank(), axis.ticks = theme_blank()) #+ xlim(0,23) + xlab("time of day")
    scale_x_datetime(date_breaks = "6 months" , date_labels = "%m-%y",expand=c(0,0)) +
    #ggtitle(paste(bin_label,"monthly",bt_str,"bin % presence")) +
    theme_bw() +
    theme(legend.title = element_text(size=12),
          legend.text = element_text(size=12),
          strip.text.y.left = element_text(angle = 0, face = "bold")) +
    theme(axis.text = element_text(size=12),
          axis.title.y = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks = element_blank(),
          panel.grid.minor = element_blank(),
          panel.grid.major = element_blank(),
          panel.border = element_blank(),
          strip.background = element_rect(colour=NA, fill=NA))+
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) #+
    #labs(fill = "% presence")
  return(out)

}







#redo M2 2009

#first figure out what labels should be deleted.Showing up that there are LM gt labels in there
whatsthis1 = dbGet("SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id WHERE soundfiles.data_collection_id = 144 AND procedure = 10")
whatsthis2 = dbGet("SELECT * FROM detections WHERE procedure = 10 AND high_freq=53.6")

#also try, see which FG have bins which intersect. Looks like some were introduced from hard negatives... these data look correct,
#leave them.

#todo: I want to reload the data, and then delete the old data if it is any different.

#way I'm going to do it is query the old (probably worthless) data, save it, and then delete it from db.

#olddat = dbGet("SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id WHERE soundfiles.data_collection_id = 144 AND procedure = 5")
#write.csv(olddat,"BS09_AU_PM02-a_data_temp.csv")
#out = table_delete(con,'detections',as.integer(olddat$id),hard_delete = TRUE)
#done!

fg= dbFetch(dbSendQuery(con,paste("SELECT bins.*,soundfiles.name,soundfiles.duration FROM soundfiles JOIN bins ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id WHERE data_collection.name = 'BS09_AU_PM02-a' AND bins.type = 1",sep="")))

#rav_og_data = read.delim(bb_path)

#put fg in cons order

fg = fg[order(fg$name,fg$seg_start),]

fg$cons = c(0,cumsum(fg$seg_end-fg$seg_start)[1:length(cumsum(fg$seg_end-fg$seg_start))-1])

times=read.csv("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Combined_sound_files/No_whiten_decimate_by_128/BS09_AU_02a_files_All_SFiles_and_durations.csv")

#use the old time calculation of sfs instead of new- new will propogate differences through file and position labels incorrectly.
justfiles = fg[-which(duplicated(fg$name)),]
justfiles$duration = times$Duration
justfiles$cons =  c(0,cumsum(justfiles$duration[1:length(justfiles$duration)-1]))

path = "//akc0ss-n086/NMML_CAEP_Acoustics/Detector/LFmoan_project/ColeAnalysis/Results/BS09_AU_02a"
files = c("01BS09_AU_02a_files_All_LM_Model_Applied_probs_Cole_Reviewed.txt",
          "02BS09_AU_02a_files_All_LM_Model_Applied_probs_Cole_Reviewed.txt",
          "03BS09_AU_02a_files_All_LM_Model_Applied_probs_Cole_Reviewed.txt",
          "04BS09_AU_02a_files_All_LM_Model_Applied_probs_Cole_Reviewed.txt")
coldat = list()

for(i in 1:length(files)){

  coldat[[i]] = read.delim(paste(path,files[i],sep="/"))
  coldat[[i]] = coldat[[i]][order(coldat[[i]]$Begin.Time..s.),]
  coldat[[i]]$chunk = i
}

coldat = do.call('rbind',coldat)

coldat$File_format = paste("AU-BSPM02_a",substr(coldat$File,9,nchar(coldat$File)),sep="")

#get the db sf durations
coldat$file_dur = fg[match(coldat$File_format,fg$name),"duration"]


#ajust the times. found them in SFiles in \\akc0ss-n086\NMML_CAEP_Acoustics\Detector\Combined_sound_files\No_whiten_decimate_by_128, copied by hand.
coldat[which(coldat$chunk==2),"Begin.Time..s."]= coldat[which(coldat$chunk==2),"Begin.Time..s."] + 4297257.37
coldat[which(coldat$chunk==3),"Begin.Time..s."]= coldat[which(coldat$chunk==3),"Begin.Time..s."] + 4297257.37 + 4297294.94
coldat[which(coldat$chunk==4),"Begin.Time..s."]= coldat[which(coldat$chunk==4),"Begin.Time..s."] + 4297257.37 + 4297294.94 +4297273.84

coldat[which(coldat$chunk==2),"End.Time..s."]= coldat[which(coldat$chunk==2),"End.Time..s."] + 4297257.37
coldat[which(coldat$chunk==3),"End.Time..s."]= coldat[which(coldat$chunk==3),"End.Time..s."] + 4297257.37 + 4297294.94
coldat[which(coldat$chunk==4),"End.Time..s."]= coldat[which(coldat$chunk==4),"End.Time..s."] + 4297257.37 + 4297294.94 +4297273.84

coldat$dur = coldat$End.Time..s.-coldat$Begin.Time..s.

coldat$start_file = justfiles$soundfiles_id[findInterval(coldat$Begin.Time..s.,justfiles$cons)]
coldat$end_file = justfiles$soundfiles_id[findInterval(coldat$End.Time..s.,justfiles$cons)]

coldat$Begin.Time..s. = coldat$Begin.Time..s. - justfiles$cons[findInterval(coldat$Begin.Time..s.,justfiles$cons)]
coldat$End.Time..s. = coldat$End.Time..s. - justfiles$cons[findInterval(coldat$End.Time..s.,justfiles$cons)]

coldat$tempid = 1:nrow(coldat)

#have all the info I need to submit. But, I should put a temp id on these, so I can modify after with the post
#review changes (Dan files).

template = dbFetch(dbSendQuery(con,'SELECT * FROM detections LIMIT 1'))

input = data.frame(coldat$Begin.Time..s.,coldat$End.Time..s.,coldat$Low.Freq..Hz.,coldat$High.Freq..Hz.,
                   coldat$start_file,coldat$end_file,coldat$LM.prob,coldat$Comments,5,coldat$Verification,3,2)

colnames(input)=colnames(template)[2:(length(input)+1)]

lab_id = lookup_from_match(con,"label_codes",unique(input$label),"alias")
input$label = lab_id$id[match(input$label,lab_id$alias)]

#alright, can submit this.

#dbAppendTable(con,'detections',input)

#alright, do the same for 'dan' detections.
path = "//akc0ss-n086/NMML_CAEP_Acoustics/Detector/LFmoan_project/ColeAnalysis/Results/BS09_AU_02a"
files = c("01BS09_AU_02a_files_All_LM_Model_Applied_probs_Cole_Reviewed_Dan_Reviewed.txt",
          "02BS09_AU_02a_files_All_LM_Model_Applied_probs_Cole_Reviewed_Dan_Reviewed.txt",
          "03BS09_AU_02a_files_All_LM_Model_Applied_probs_Cole_Reviewed_Dan_Reviewed.txt",
          "04BS09_AU_02a_files_All_LM_Model_Applied_probs_Cole_Reviewed_Dan_Reviewed.txt")
dandat = list()

for(i in 1:length(files)){

  dandat[[i]] = read.delim(paste(path,files[i],sep="/"))
  dandat[[i]] = dandat[[i]][order(dandat[[i]]$Begin.Time..s.),]
  dandat[[i]]$chunk = i
}

dandat = do.call('rbind',dandat)

dandat$File_format = paste("AU-BSPM02_a",substr(dandat$File,9,nchar(dandat$File)),sep="")

#get the db sf durations
dandat$file_dur = fg[match(dandat$File_format,fg$name),"duration"]

#do manually
#times=read.csv("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Combined_sound_files/No_whiten_decimate_by_128/BS09_AU_02a_files_All_SFiles_and_durations.csv")
dandat[which(dandat$chunk==2),"Begin.Time..s."]= dandat[which(dandat$chunk==2),"Begin.Time..s."] + 4297257.37
dandat[which(dandat$chunk==3),"Begin.Time..s."]= dandat[which(dandat$chunk==3),"Begin.Time..s."] + 4297257.37 + 4297294.94
dandat[which(dandat$chunk==4),"Begin.Time..s."]= dandat[which(dandat$chunk==4),"Begin.Time..s."] + 4297257.37 + 4297294.94 +4297273.84

dandat[which(dandat$chunk==2),"End.Time..s."]= dandat[which(dandat$chunk==2),"End.Time..s."] + 4297257.37
dandat[which(dandat$chunk==3),"End.Time..s."]= dandat[which(dandat$chunk==3),"End.Time..s."] + 4297257.37 + 4297294.94
dandat[which(dandat$chunk==4),"End.Time..s."]= dandat[which(dandat$chunk==4),"End.Time..s."] + 4297257.37 + 4297294.94 +4297273.84


dandat$dur = dandat$End.Time..s.-dandat$Begin.Time..s.

dandat$start_file = justfiles$soundfiles_id[findInterval(dandat$Begin.Time..s.,justfiles$cons)]
dandat$end_file = justfiles$soundfiles_id[findInterval(dandat$End.Time..s.,justfiles$cons)]

dandat$Begin.Time..s. = dandat$Begin.Time..s. - justfiles$cons[findInterval(dandat$Begin.Time..s.,justfiles$cons)]
dandat$End.Time..s. = dandat$End.Time..s. - justfiles$cons[findInterval(dandat$End.Time..s.,justfiles$cons)]

onlydiff = dandat[-which(paste(dandat$Begin.Time..s.,dandat$End.Time..s.,dandat$start_file,dandat$Verification) %in%
                                 paste(coldat$Begin.Time..s.,coldat$End.Time..s.,coldat$start_file,coldat$Verification)),]

onlydiff$procedure = 5

onlydiff = data.frame(onlydiff$Begin.Time..s.,onlydiff$End.Time..s.,onlydiff$Low.Freq..Hz.,onlydiff$High.Freq..Hz.,
                      onlydiff$start_file,onlydiff$end_file,onlydiff$LM.prob,onlydiff$Comments,5,onlydiff$Verification,3,2)

colnames(onlydiff)=colnames(template)[2:(length(onlydiff)+1)]
#for onlydiff, pull out of db to get ids

onlydiff$start_file = as.integer(onlydiff$start_file)
onlydiff$end_file = as.integer(onlydiff$end_file)

w_ids = table_dataset_lookup(con,"SELECT * FROM detections",onlydiff[,c(1,2,5,6,7,9)],
                             c("DOUBLE PRECISION","DOUBLE PRECISION","integer","integer","DOUBLE PRECISION","integer"))

w_ids = w_ids[order(w_ids$start_time),]
onlydiff = onlydiff[order(onlydiff$start_time),]

#now they match= cbind ids, and update verification.

ds = data.frame(w_ids$id,onlydiff$label)

colnames(ds)=c("id","label")

ds$label = lab_id$id[match(ds$label,lab_id$alias)]

#table_update(con,'detections',ds)

#now need to add bin labels.

#checked the signal and labels, labels get progressively more wrong as file continues indicating probably rounding problem.
#idea- delete on db and recalcualte. Instead of adding flat sum, add an incrementing difference that is = to the total dur
#minus the floor, and subtract that from the signal.

#ended up working close enough to just use the sfiles times, which are close enough with occasional hundreds place round diff.

#now get all the dets and calc bin labels.

data = dbGet("SELECT detections.* FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id WHERE soundfiles.data_collection_id = 144 AND procedure = 5 AND status = 1")

data$start_file = as.integer(data$start_file)
data$end_file = as.integer(data$end_file)

fg= dbFetch(dbSendQuery(con,paste("SELECT bins.*,soundfiles.name,soundfiles.duration FROM soundfiles JOIN bins ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id WHERE data_collection.name = 'BS09_AU_PM02-a' AND bins.type = 1",sep="")))
fg$soundfiles_id = as.integer(fg$soundfiles_id)
fg$id = as.integer(fg$id)


bins = bin_negatives(data,fg,bintype="LOW",analyst='previous',procedure = 5,signal_code =3,format='db')
#dbAppendTable(con,'detections',bins)

#looks ok!
