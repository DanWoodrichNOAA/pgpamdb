
#fw proj functions

#' Format FW data on ANALYSIS
#'
#' Format the FW data on ANALYSIS into a db ready format
#' @param conn The database connection
#' @param mooringname The name of the dataset as on ANALYSIS. Can be new naming or old naming convention (must match data_collection.name or data_collection.historic_name)
#' @return a dataframe ready to be loaded into db.
#' @export
format_FW = function(conn,mooringname){

  #determine if results exists

  if(!dir.exists(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",mooringname,sep=""))){
    stop("mooring name not recognized on ANALYSIS")
  }

  newname = dbFetch(dbSendQuery(conn,paste("SELECT COUNT(*) FROM data_collection WHERE name ='",mooringname,"'",sep="")))$count==1
  oldname=  dbFetch(dbSendQuery(conn,paste("SELECT COUNT(*) FROM data_collection WHERE historic_name ='",mooringname,"'",sep="")))$count==1

  if(newname == TRUE){
    newname = mooringname
    oldname = dbFetch(dbSendQuery(conn,paste("SELECT historic_name FROM data_collection WHERE name ='",mooringname,"'",sep="")))$historic_name
  }else if(oldname== TRUE){
    newname = dbFetch(dbSendQuery(conn,paste("SELECT name FROM data_collection WHERE historic_name ='",mooringname,"'",sep="")))$name
    oldname = mooringname
  }else{
    stop("mooring name does not have match in data_collection table in database")
  }

  #determine if results folder exists:

  if(!dir.exists(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",mooringname,"/Detector/TPRthresh_0.99/FD",sep=""))){
    stop("FG results dir not found on ANALYSIS")
  }

  if(file.exists(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",mooringname,
                       "/Detector/TPRthresh_0.99/FD/",newname,"_MarkingsTab.txt",sep=""))){

    FNdat =read.delim(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",mooringname,
                            "/Detector/TPRthresh_0.99/FN/",newname,"_files_All_FN_Model_Applied_probs.txt",sep=""))

    BBdat =read.delim(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",mooringname,
                            "/Detector/TPRthresh_0.99/BB/",newname,"_files_All_BB_Model_Applied_probs.txt",sep=""))

    FDreslt = read.delim(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",mooringname,
                               "/Detector/TPRthresh_0.99/FD/",newname,"_files_All_FD_Model_Applied_probs.txt",sep=""))
    MarkTab = read.delim(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",mooringname,
                               "/Detector/TPRthresh_0.99/FD/",newname,"_MarkingsTab.txt",sep=""))
  }else{

    FNdat =read.delim(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",mooringname,
                            "/Detector/TPRthresh_0.99/FN/",oldname,"_files_All_FN_Model_Applied_probs.txt",sep=""))

    BBdat =read.delim(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",mooringname,
                            "/Detector/TPRthresh_0.99/BB/",oldname,"_files_All_BB_Model_Applied_probs.txt",sep=""))

    FDreslt = read.delim(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",mooringname,
                               "/Detector/TPRthresh_0.99/FD/",oldname,"_files_All_FD_Model_Applied_probs.txt",sep=""))
    MarkTab = read.delim(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",mooringname,
                               "/Detector/TPRthresh_0.99/FD/",oldname,"_MarkingsTab.txt",sep=""))
  }

  FNdat = FNdat[which(FNdat$FN.prob>=.91),]
  BBdat = BBdat[which(BBdat$BB.prob>=.90),]

  colnames(FNdat)[which(colnames(FNdat)=='FN.prob')]="probs"

  if(nrow(FNdat)>0){

    FNdat$CallType = "FN"
    FNdat$label= 'pn'
  }


  #FNcompare = paste(round(FNdat$Begin.Time..s.,2),FNdat$combinedFileNum) %in% paste(round(FDreslt$Begin.Time..s.,2),FDreslt$combinedFileNum)
  FNcompare= paste(FNdat$Selection,FNdat$CallType) %in% paste(FDreslt$Selection,FDreslt$CallType)

  if(length(FNcompare)>0){
    FNdat = FNdat[which(!FNcompare),]
  }

  colnames(BBdat)[which(colnames(BBdat)=='BB.prob')]="probs"

  if(nrow(BBdat)>0){

    BBdat$CallType = "BB"
    BBdat$label= 'pn'
  }

  #end time noted to be unstable on at least one mooring: "AW13_AU_BS1". adding 2 decimal rounding and using probability instead of end time.
  #unable to figure out solution to rounding issue- try just with selection as id?
  #BBcompare = paste(round(BBdat$Begin.Time..s.,2),BBdat$combinedFileNum) %in% paste(round(FDreslt$Begin.Time..s.,2),FDreslt$combinedFileNum)
  BBcompare = paste(BBdat$Selection,BBdat$CallType) %in% paste(BBdat$Selection,BBdat$CallType)


  if(length(BBcompare)>0){
    BBdat = BBdat[which(!BBcompare),]
  }




  if(!is.na(FDreslt$Selection[1])){
    FDreslt$label = 'py'
    #colnames(FDreslt) = c(colnames(FDreslt),"CallType","label")
    if(nrow(FNdat)>0 & nrow(BBdat)>0){
      FDreslt = rbind(FDreslt,BBdat,FNdat)
    }else if(nrow(FNdat)>0){
      FDreslt = rbind(FDreslt,FNdat)
    }else if(nrow(BBdat)>0){
      FDreslt = rbind(FDreslt,BBdat)
    }
  }else{
    FDreslt = rbind(BBdat,FNdat)
  }

  #ok, now should have all of the data we need. How to associate file names here with ids on db? Convert to datetime, and
  #query ids based on that.

  #convert FDreslt to datetime

  template = dbFetch(dbSendQuery(conn,"SELECT * FROM detections LIMIT 0"))

  data_sources = vector('list',4) #this will be all data sources at end to combine. nulls are ok.

  if(nrow(FDreslt)>0){

    FDreslt$File_dt = as.POSIXct(substr(FDreslt$File,nchar(FDreslt$File)-16,nchar(FDreslt$File)-4),format="%y%m%d-%H%M%S",tz="UTC")

    #get all of the soundfile ids and dts

    query = gsub("[\r\n]", "",paste("SELECT soundfiles.id,duration,datetime FROM soundfiles JOIN data_collection ON data_collection.id
                                  = soundfiles.data_collection_id WHERE data_collection.name = '",newname,"'",sep=""))

    sf_info = dbFetch(dbSendQuery(conn,query))

    #now need to associate the ids with each detection. This corresponds to start file, need to also determine end file.

    sf_info=sf_info[order(sf_info$datetime),]

    sf_info$cumsum = c(0,cumsum(sf_info$duration)[1:nrow(sf_info)-1])#-sf_info$duration[1]

    sf_info$id = as.integer(sf_info$id)

    #order FDreslt in order of soundfile datetime, then file offest

    FDreslt = FDreslt[order(FDreslt$File_dt,FDreslt$FileOffsetBegin),]

    FDreslt$sf_id = sf_info[match(FDreslt$File_dt,sf_info$datetime),"id"]

    FDreslt$det_dur =FDreslt$End.Time..s.-FDreslt$Begin.Time..s.

    FDreslt$Begin.Time..s. = sf_info[match(FDreslt$sf_id,sf_info$id),"cumsum"]+ FDreslt$FileOffsetBegin

    FDreslt$End.Time..s. = FDreslt$Begin.Time..s + FDreslt$det_dur

    FDreslt$FileOffsetEnd = FDreslt$FileOffsetBegin + FDreslt$det_dur

    nextfilez_start = which(FDreslt$Begin.Time..s.>sf_info[match(FDreslt$sf_id,sf_info$id)+1,"cumsum"]) #plus 1 to index is fine because we assume sf_info is in order of datetime.

    FDreslt$start_file = FDreslt$sf_id
    FDreslt$end_file = FDreslt$sf_id

    if(length(nextfilez_start)>0){

      FDreslt$FileOffsetBegin[nextfilez_start]= FDreslt$FileOffsetBegin[nextfilez_start]-sf_info[match(FDreslt[nextfilez_start,"sf_id"],sf_info$id),"duration"]
      FDreslt$start_file[nextfilez_start] = sf_info[match(FDreslt[nextfilez_start,"sf_id"],sf_info$id)+1,"id"]

    }

    nextfilez_end = which(FDreslt$End.Time..s.>sf_info[match(FDreslt$sf_id,sf_info$id)+1,"cumsum"])

    if(length(nextfilez_end)>0){

      #here, %in% is ok to reassign since we can assume both FDreslt and sf_info are in consecutive order.
      FDreslt$FileOffsetEnd[nextfilez_end]= FDreslt$FileOffsetEnd[nextfilez_end]-sf_info[match(FDreslt[nextfilez_end,"sf_id"],sf_info$id),"duration"]
      FDreslt$end_file[nextfilez_end] = sf_info[match(FDreslt[nextfilez_end,"sf_id"],sf_info$id)+1,"id"]

    }

    #cool. Do we have all info we need to assemble detections data type? Let's give it a try

    ct_lookup = dbFetch(dbSendQuery(conn,"SELECT id,code FROM signals WHERE code IN ('BB','FN','FW','AG')"))

    ct_lookup$id = as.integer(ct_lookup$id)

    lab_lookup = dbFetch(dbSendQuery(conn,"SELECT id,alias FROM label_codes WHERE alias IN ('py','pn')"))

    outdata = data.frame(FDreslt$FileOffsetBegin,FDreslt$FileOffsetEnd,FDreslt$Low.Freq..Hz.,FDreslt$High.Freq..Hz.,
                         FDreslt$start_file,FDreslt$end_file,FDreslt$probs,FDreslt$Comment,6,lab_lookup$id[match(FDreslt$label,lab_lookup$alias)],ct_lookup$id[match(FDreslt$CallType,ct_lookup$code)],2,FDreslt$File_dt)

    colnames(outdata)= c(colnames(template[2:13]),'datetime')

    lab_lookup$upgraded = dbFetch(dbSendQuery(conn,"SELECT id FROM label_codes WHERE alias IN ('y','n')"))$id

    #now- assign human labels. Loop through marktab, and assign highest detection in hour 'upgraded' label.

    #first, truncate datetime to hr

    outdata$datetime=format(outdata$datetime,"%y%m%d %H")

    #now, loop through every

    outdata$temp_id = 1:nrow(outdata)

    MarkTab_reduce = MarkTab[-which(duplicated(paste(MarkTab$IDvec,MarkTab$Species.i.))),]

    ag_dataset = list()
    ag_counter = 1
    for(i in 1:nrow(MarkTab_reduce)){

      row = outdata[which((outdata$datetime %in% MarkTab[which(((MarkTab$IDvec == MarkTab_reduce$IDvec[i]) & (MarkTab$Species.i. == MarkTab_reduce$Species.i.[i]))),"V1"]) & (outdata$signal_code %in% ct_lookup$id[match(MarkTab_reduce$Species.i.[i],ct_lookup$code)])),]
      row = row[which.max(row$probability),]

      if(nrow(row)>0){
        if((row$label==21 & MarkTab_reduce$MarkVec[i]=='n') | (row$label==20 & MarkTab_reduce$MarkVec[i]=='y')){
          stop('assumption error- labels conflict between original designation and back-calculated designation')
        }

        outdata[which(outdata$temp_id==row$temp_id),"label"]=row$label-20 #assumes numeric relationship between ids... careful


        if(MarkTab_reduce$MarkVec[i]=='a'){

          #if airgun data, want to duplicate detections and append as positive for airguns under a different procedure.

          #change signal code to AG and change labels to positive labels.
          ag_data = outdata[which((outdata$datetime %in% MarkTab[which(((MarkTab$IDvec == MarkTab_reduce$IDvec[i]) & (MarkTab$Species.i. == MarkTab_reduce$Species.i.[i]))),"V1"]) & (outdata$signal_code %in% ct_lookup$id[match(MarkTab_reduce$Species.i.[i],ct_lookup$code)])),]

          ag_data$signal_code=ct_lookup$id[which(ct_lookup$code=="AG")]
          ag_data$label= ag_data$label+1 #assumes numeric relationship between ids... careful
          ag_data$procedure=8

          ag_data$comments=paste('#fp_of_:',MarkTab_reduce$Species.i.[i],sep="")

          ag_dataset[[ag_counter]]=ag_data

          ag_counter=ag_counter+1

        }

      }



    }

    #cool, now
    #reduce a working set to just the y and py
    #loop through each soundfile in mooring.
    #inner loop to forge negative detections.



    outdata$temp_id=NULL
    outdata$datetime=NULL

    data_sources[[1]]=outdata

    onlyyes = outdata[which(outdata$label %in% c(1,21)),]

    if(length(ag_dataset)>0){
      ag_dataset = do.call('rbind',ag_dataset)

      ag_dataset = ag_dataset[order(ag_dataset$datetime),]
      ag_dataset$datetime=NULL
      ag_dataset$temp_id=NULL


    }

    sf_no_dets = sf_info[which(!sf_info$id %in% c(onlyyes$start_file,onlyyes$end_file)),]

  }else{

    sf_no_dets = sf_info

  }
  #template_row = outdata[1,]



  if(nrow(sf_no_dets)>0){
    no_dets_frame = data.frame(0,sf_no_dets$duration,0,64,sf_no_dets$id,sf_no_dets$id,NA,"",7,20,ct_lookup$id[which(ct_lookup$code=="FW")],1)

    colnames(no_dets_frame)=colnames(template[2:13])

    data_sources[[3]]=no_dets_frame
  }

  sf_info_w_dets = sf_info[which(!sf_info$id %in%sf_no_dets$id),]

  rows = list()

  counter = 1

  if(nrow(sf_info_w_dets)>0){

    for(i in 1:nrow(sf_info_w_dets)){


      #thinking here- I think that instead of filling negative space, I should just create
      #FW n detections on the low bin scale. Reason being, is that there will be fewer errors to correct
      #since there was no expectation that yeses would be comprehensive, just comprehensive on the low bin time scale.
      #i could perhaps submit FW y detections, but a little redundant for most cases- but it would make comparisons easier.
      #don't think that's worth it, since it creates a lot of detections to maintain (redundantly) and I could just do this
      #same comparison on the other end.

      #subset detections in sound file (don't forget to find overlapping start and ends)
      #create detections based on intervals

      endtimes = onlyyes[which(onlyyes$end_file==sf_info_w_dets$id[i]),"end_time"]
      starttimes = onlyyes[which(onlyyes$start_file==sf_info_w_dets$id[i]),"start_time"]

      #want to stay at LOW bin size for consistency- so,
      bins = seq(from=0,to=sf_info_w_dets$duration[i],by=300)

      bins= bins[which(bins!=sf_info_w_dets$duration[i])]

      yes_bins = unique(findInterval(c(endtimes,starttimes),bins))
      no_bins = bins[-yes_bins]

      if(length(no_bins)>0){

        intab = list()

        for(j in 1:length(no_bins)){

          inrow = c(no_bins[j],no_bins[j]+300,0,64,sf_info_w_dets$id[i],sf_info_w_dets$id[i],NA,"",7,20,ct_lookup$id[which(ct_lookup$code=="FW")],1)
          intab[[j]]=inrow
        }

        intab = do.call('rbind',intab)

        #make sure bins don't exceed duration
        intab[,2][which(intab[,2]>sf_info_w_dets$duration[i])]=sf_info_w_dets$duration[i]

        rows[[counter]] = intab

        counter = counter + 1

      }

    }

    if(length(rows)>0){
      rows = do.call('rbind',rows)

      colnames(rows)=colnames(template[2:13])

      data_sources[[4]] = rows

    }




  }

  all_dets =do.call('rbind',data_sources)


  #alright, this is my object to submit

  return(all_dets)

  #before I start submitting data- need to test to make sure designated labels match actual labels.
  #so, take a few examples in detectorrunoutput, and see if they match up

  #how to determine? idea is to load them in, and simply take the detection intervals (end- start)  label and see if they
  #match what we determined here.


  #candidates to compare:
  #1:\\akc0ss-n086\NMML_CAEP_Acoustics\Detector\DetectorRunOutput\FinReview_20221111113632
  #above soundfiles are not yet loaded to database
  #2: (has airguns!) \\akc0ss-n086\NMML_CAEP_Acoustics\Detector\DetectorRunOutput\FinReview_20220217091410
  #3.\\akc0ss-n086\NMML_CAEP_Acoustics\Detector\DetectorRunOutput\FinReview_20200817090230

}

#compare back calculated labels against actual labels.

#2: (has airguns!) \\akc0ss-n086\NMML_CAEP_Acoustics\Detector\DetectorRunOutput\FinReview_20220217091410

#3.\\akc0ss-n086\NMML_CAEP_Acoustics\Detector\DetectorRunOutput\FinReview_20200817090230

#' Check FW labels
#'
#' Check FW labels against original labels
#' @param conn The database connection
#' @param calcdata data output from format_FW
#' @param original_folder the folder name containing original labels (output folder for FinReview.R)
#' @return a table which shows original and back calculated labels side by side
#' @export
FN_validate = function(conn,calcdata,original_folder){

  labtab_bc = calcdata[which((calcdata$label %in% c(0,1)) & (calcdata$signal_code %in% c(5,6))),]

  infiles = dir(original_folder)[substr(dir(original_folder),1,1)=="0"]

  tabs = list()
  for(i in 1:length(infiles)){

    tabs[[i]] =read.delim(paste(original_folder,"/",infiles[i],sep=""))

  }

  reallabs = do.call('rbind',tabs)

  reallabs = reallabs[order(reallabs$File,reallabs$Begin.Time..s.),]

  calcvec = as.numeric(labtab_bc$end_time)-as.numeric(labtab_bc$start_time)
  realvec = reallabs$End.Time..s.-reallabs$Begin.Time..s.

  #calc_hl =as.numeric(labtab_bc$high_freq)-as.numeric(labtab_bc$low_freq)
  #real_hl =reallabs$High.Freq..Hz.-reallabs$Low.Freq..Hz.

  file_query = dbFetch(dbSendQuery(conn,paste("SELECT id,datetime FROM soundfiles WHERE id IN (",paste(labtab_bc$start_file,collapse=",",sep=""),")",sep="")))

  file_query$id = as.integer(file_query$id)
  labtab_bc$file = file_query$datetime[match(labtab_bc$start_file,file_query$id)]

  test = data.frame(labtab_bc$file,calcvec,labtab_bc$label,labtab_bc$signal_code,reallabs$File,realvec,reallabs$Verification,reallabs$CallType)

  if('a' %in% reallabs$Verification){

    labtab_bc_ag = calcdata[which((calcdata$label == 1) & (calcdata$signal_code == 21)),]

    file_query = dbFetch(dbSendQuery(conn,paste("SELECT id,datetime FROM soundfiles WHERE id IN (",paste(labtab_bc_ag$start_file,collapse=",",sep=""),")",sep="")))

    file_query$id = as.integer(file_query$id)
    labtab_bc_ag$file = file_query$datetime[match(labtab_bc_ag$start_file,file_query$id)]

    #labtab_bc_ag=labtab_bc_ag[order(labtab_bc_ag$start_file,labtab_bc_ag$start_time),]
    reallabs_ag = reallabs[which(reallabs$Verification=='a'),]

    calcvec = as.numeric(labtab_bc_ag$end_time)-as.numeric(labtab_bc_ag$start_time)
    realvec = reallabs_ag$End.Time..s.-reallabs_ag$Begin.Time..s.

    test2 = data.frame(labtab_bc_ag$file,calcvec,labtab_bc_ag$label,labtab_bc_ag$signal_code,reallabs_ag$File,realvec,reallabs_ag$Verification,reallabs_ag$CallType)

    colnames(test2) = colnames(test)

    test = rbind(test,test2)
  }

  return(test)

}

#' @import utils
NULL
