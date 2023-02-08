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


#don't move this- most useful for old conversion..?
fg_breakbins <-function(data,interval){

  out_bins = list()

  for(n in 1:nrow(data)){

    a = data$SegStart[n]
    b = (data$SegDur[n]+data$SegStart[n])
    c = interval

    breaks = as.numeric(unique(cut(c((a+0.000001):(b-0.000001)), seq(0, 100000, by=c), include.lowest = F)))

    start = (breaks-1)*c
    end = breaks*c

    start[1] = a
    end[length(end)] = b
    #print(cbind(start,end))

    dur = end-start

    if("SiteID" %in% colnames(data)){

      out_ = data.frame(data$FileName[n],data$FullPath[n],data$StartTime[n],data$Duration[n],data$Deployment[n],start,dur,data$SiteID[n])

    }else{

      out_ = data.frame(data$FileName[n],data$FullPath[n],data$StartTime[n],data$Duration[n],data$Deployment[n],start,dur)

    }

    colnames(out_) = colnames(data)
    out_bins[[n]] = out_
  }

  fg_broke= do.call('rbind',out_bins)

  return(fg_broke)


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

#don't submit- needs a rewrite to db standard.
bin_negatives<-function(data,FG,bintype,analyst){

  out_negs = vector("list",2)

  selection = which(c("LOW","REG","SHI")==bintype)

  interval = c(300,225,90)[selection]
  highfreq = c(512,8192,16384)[selection]

  #originally based on part of the fin whale data loader.
  #two part processes- first just takes the files without any detections,
  #and assumes them to be negative. The other takes the files with detections, and splice them into bins
  #to determine which are negative bins.

  #assume the data comes in is in detx format.

  #for this comparison, only need the 'y' label detections

  data = data[which(data$label=='y' | data$label=='py'),]

  sfs_w_yes = unique(data$StartFile,data$EndFile)

  fg_w_no = FG[-which(FG$FileName %in% sfs_w_yes),]

  if(nrow(fg_w_no)>0){

    no_dets_frame = data.frame(0,fg_w_no$Duration,0,highfreq,fg_w_no$FileName,fg_w_no$FileName,NA,highfreq,'pn',"",data$SignalCode[1],"DET",9999,analyst)

    colnames(no_dets_frame)=colnames(data)

    if(any(duplicated(no_dets_frame))){
      no_dets_frame = no_dets_frame[-which(duplicated(no_dets_frame)),]
    }

    out_negs[[1]]=no_dets_frame

  }

  #now go through the file which do have detections, and determine the negative bins.

  fg_w_yes = FG[which(FG$FileName %in% sfs_w_yes),]



  if(nrow(fg_w_yes)>0){

    #make sure fg_w_yes is broken into correct interval for this
    fg_w_yes = fg_breakbins(fg_w_yes,interval)

    rows = list()

    counter = 0

    #loop through each row

    for(i in 1:nrow(fg_w_yes)){
      #this is now quite simple- if there are any start or end times within the fg row, call it yes, otherwise spit out no.

      endtimes = data[which(data$EndFile==fg_w_yes$FileName[i] & ((data$EndTime<fg_w_yes$SegStart[i]+fg_w_yes$SegDur[i]) & (data$EndTime>fg_w_yes$SegStart[i]))),"EndTime"]
      starttimes = data[which(data$StartFile==fg_w_yes$FileName[i] & ((data$StartTime<fg_w_yes$SegStart[i]+fg_w_yes$SegDur[i]) & (data$StartTime>fg_w_yes$SegStart[i]))),"StartTime"]

      if(length(endtimes)==0 & length(starttimes)==0){
        #no detection, so bin is a pn
        counter = counter + 1

        rows[[i]] = c(fg_w_yes$SegStart[i],fg_w_yes$SegStart[i]+fg_w_yes$SegDur[i],0,highfreq,fg_w_yes$FileName[i],fg_w_yes$FileName[i],NA,highfreq,'pn',"",data$SignalCode[1],"DET",9999,analyst)
      }
    }


    if(length(rows)>0){
      rows = do.call('rbind',rows)

      colnames(rows)=colnames(data)

      out_negs[[2]] = rows

    }
  }

  return(do.call("rbind",out_negs))


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
