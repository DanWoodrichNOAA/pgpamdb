#function to populate database with soundfiles.

#connect to db

#' Pamdb connect
#'
#' Connect to the pamdb database, using locally available files.
#' @param dbname The name of the db
#' @param infoScript The script that when run, populates dsn_ variables in local function
#' @param sslKey_path The path to the sslKey
#' @param sslCert_path The path to the sslCert
#' @return The database connection
#' @export

pamdbConnect<-function(dbname,infoScript,sslKey_path,sslCert_path){

  cfg = source(infoScript,local = TRUE)

  con <- dbConnect(RPostgres::Postgres(),
                   dbname = dbname,
                   host = dsn_hostname,
                   port = dsn_port,
                   user = dsn_uid,
                   password = dsn_pwd,
                   sslmode = 'require',
                   sslkey= sslKey_path,
                   sslcert = sslCert_path)

  return(con)

}

#helper fxns for db format data and process

#' break FG into n sized bins
#'
#' return a vers of a filegroup which has segments broken into sections of size interval.
#' @param data The filegroup data.
#' @param interval size in seconds for max bins.
#' @param format detx or db format
#' @return FG with rows greater than interval broken into interval sized segments.
#' @export
fg_breakbins <-function(data,interval,format='db'){

  out_bins = list()

  for(n in 1:nrow(data)){

    if(format=="DETx"){
      a = data$SegStart[n]
      b = (data$SegDur[n]+data$SegStart[n])
    }else if(format=="db"){
      a = data$seg_start[n]
      b = data$seg_end[n]
    }

    c = interval

    breaks = as.numeric(unique(cut(c((a+0.000001):(b-0.000001)), seq(0, 100000, by=c), include.lowest = F)))

    start = (breaks-1)*c
    end = breaks*c

    start[1] = a
    end[length(end)] = b
    #print(cbind(start,end))

    dur = end-start

    if(format=="DETx"){

      if("SiteID" %in% colnames(data)){

        out_ = data.frame(data$FileName[n],data$FullPath[n],data$StartTime[n],data$Duration[n],data$Deployment[n],start,dur,data$SiteID[n])

      }else{

        out_ = data.frame(data$FileName[n],data$FullPath[n],data$StartTime[n],data$Duration[n],data$Deployment[n],start,dur)

      }

      colnames(out_) = colnames(data)

    }else if(format=="db"){

      #out_ = data.frame(data$name,data$duration,data$name..5,start,end)
      out_ = data.frame(data$soundfiles_id[n],start,end)

      #just make this spit out minimal needed
      colnames(out_) = c("soundfiles_id","seg_start","seg_end")

    }

    out_bins[[n]] = out_
  }

  fg_broke= do.call('rbind',out_bins)

  return(fg_broke)
}

#' produce bin negatives
#'
#' create data frame of bin negatives for a detector deployment on arbitrary fg
#' @param data The detection data
#' @param FG The filegroup
#' @param bintype "LOW","REG", or "SHI" . Will automatically break bins so that it is on best timescale for queries.
#' @param procedure procedure id to populate in output object. defaults to procedure in 1st row if provided in data.
#' @param signal_code signal_code to populate in output object. defaults to signal_code in 1st row if provided in data.
#' @param format detx or db format
#' @return data frame of bin negatives.
#' @export
bin_negatives<-function(data,FG,bintype,analyst='previous',procedure = NULL,signal_code =NULL,format='db'){

  out_negs = vector("list",2)

  selection = which(c("LOW","REG","SHI")==bintype)

  interval = c(300,225,90)[selection]
  highfreq = c(512,8192,16384)[selection]

  #originally based on part of the fin whale data loader.
  #two part processes- first just takes the files without any detections,
  #and assumes them to be negative. The other takes the files with detections, and splice them into bins
  #to determine which are negative bins.

  #assume the data comes in is in detx format.

  if(format=='db'){
    yes_ = 1
    yes_2 = 21
    no_2 = 20

    sf_col= 'start_file'
    ef_col= 'end_file'

    et_col = 'end_time'
    st_col = 'start_time'

    if('name' %in% FG){
      fgfn = "name"
    }else{
      fgfn = "soundfiles_id"
    }


    colind_remove = c()


    if(is.null(analyst)){
      analyst="placeholder"
      colind_remove = c(colind_remove,13)

    }else if(analyst=='previous'){
      analyst = data$analyst[1]
    }

    if(is.null(procedure)){

      procedure= data$procedure[1]
    }

    if(is.null(signal_code)){

      signal_code= data$signal_code[1]
    }

  }else if(format=='DETx'){

    yes_ = 'y'
    yes_2 = 'py'
    no_2 = 'pn'

    sf_col= 'StartFile'
    sf_col= 'EndFile'

    et_col = 'StartTime'
    st_col = 'EndTime'

    fgfn = "FileName"

    if(analyst=='previous'){
      analyst = data$LastAnalyst[1]
    }

    if(is.null(procedure)){

      stop("procedure must be specific for DETx")
    }

    if(is.null(signal_code)){

      signal_code= data$SignalCode[1]
    }


  }

  #for this comparison, only need the 'y' label detections

  data = data[which(data$label==yes_ | data$label==yes_2),]

  sfs_w_yes = unique(data[,sf_col],data[,ef_col])

  if(length(sfs_w_yes)>0){
    fg_w_no = FG[-which(FG[,fgfn] %in% sfs_w_yes),]
  }else{
    fg_w_no = fg
  }


  if(nrow(fg_w_no)>0){

    if(format=='DETx'){
      no_dets_frame = data.frame(0,fg_w_no$Duration,0,highfreq,fg_w_no[,fgfn],fg_w_no[,fgfn],NA,highfreq,'pn',"",signal_code,"DET",9999,analyst)

      colnames(no_dets_frame)=colnames(data)
    }else if(format=='db'){
      no_dets_frame = data.frame(0,fg_w_no$duration,0,highfreq,fg_w_no[,fgfn],fg_w_no[,fgfn],NA,"",procedure,no_2,signal_code,2,analyst)
      colnames(no_dets_frame)=c("start_time","end_time","low_freq","high_freq","start_file","end_file",
                                "probability","comments","procedure","label","signal_code","strength","analyst")
      if(length(colind_remove)>0){
        no_dets_frame = no_dets_frame[,-colind_remove]
      }
    }

    if(any(duplicated(no_dets_frame))){
      no_dets_frame = no_dets_frame[-which(duplicated(no_dets_frame)),]
    }

    out_negs[[1]]=no_dets_frame

  }

  #now go through the file which do have detections, and determine the negative bins.

  fg_w_yes = FG[which(FG[,fgfn] %in% sfs_w_yes),]



  if(nrow(fg_w_yes)>0){

    #make sure fg_w_yes is broken into correct interval for this
    #to do: also make this function work with db fg names/types.
    fg_w_yes = fg_breakbins(fg_w_yes,interval,format = format)

    rows = list()

    counter = 0

    #loop through each row

    for(i in 1:nrow(fg_w_yes)){
      #this is now quite simple- if there are any start or end times within the fg row, call it yes, otherwise spit out no.

      if(format =="db"){
        st = fg_w_yes$seg_start[i]
        et =fg_w_yes$seg_end[i]
      }else if(format=='DETx'){
        st = fg_w_yes$SegStart[i]
        et =fg_w_yes$SegStart[i]+fg_w_yes$SegDur[i]
      }

      endtimes = data[which(data[,ef_col]==fg_w_yes[,fgfn][i] & ((data[,et_col]<et) & (data[,et_col]>st))),"EndTime"]
      starttimes = data[which(data[,sf_col]==fg_w_yes[,fgfn][i] & ((data[,st_col]<et) & (data[,st_col]>st))),"StartTime"]

      if(length(endtimes)==0 & length(starttimes)==0){
        #no detection, so bin is a pn
        counter = counter + 1


        if(format=='DETx'){
          rows[[i]] = c(st,et,0,highfreq,fg_w_yes[,fgfn][i],fg_w_yes[,fgfn][i],NA,highfreq,'pn',"",data$SignalCode[1],"DET",9999,analyst)

        }else if(format=='db'){
          rows[[i]] = c(st,et,0,highfreq,fg_w_yes[,fgfn][i],fg_w_yes[,fgfn][i],NA,"",procedure,no_2,signal_code,2,analyst)

        }


      }
    }


    if(length(rows)>0){
      rows = do.call('rbind',rows)
      if(format=='DETx'){
        colnames(rows)=colnames(data)
      }else if(format=='db'){
        if(length(colind_remove)>0){
          colnames(rows)=c("start_time","end_time","low_freq","high_freq","start_file","end_file",
                           "probability","comments","procedure","label","signal_code","strength","analyst")[-colind_remove]
        }else{
          colnames(rows)=c("start_time","end_time","low_freq","high_freq","start_file","end_file",
                           "probability","comments","procedure","label","signal_code","strength","analyst")
        }
      }
      out_negs[[2]] = rows

    }
  }

  return(do.call("rbind",out_negs))


}

#' Update implied negatives
#'
#' Update the implied negatives of a filegroup-procedure-signal_code combination
#' @param conn The database connection
#' @param fgname The name of the filegroup.
#' @param procedure procedure id for the filegroup-procedure-signal_code combination
#' @param signal_code signal id for the filegroup-procedure-signal_code combination
#' @param high_freq high frequency for implied negatives. will search the db for indications of what this should be if not provided.
#' @return number of rows inserted corresponding to new implied negative detections.
#' @export
i_neg_update <- function(conn,fgname,procedure,signal_code,high_freq = NULL){

  #make sure it has i_neg assumption

  query = paste("SELECT * FROM effort_procedures JOIN effort ON effort_procedures.effort_id = effort.id
  WHERE procedures_id = ",procedure," AND signal_code = ",signal_code," AND effort.name = '",fgname,"'",sep="")

  query <- gsub("[\r\n]", "", query)

  row = dbFetch(dbSendQuery(conn,query))

  if(row$effproc_assumption!='i_neg'){
    stop("cannot confirm the section of effort as i_neg assumption on select procedure and signal code. Add entry to
         effort_procedures if assumption is correct")
  }
  #

  dets = dbFetch(dbSendQuery(conn,paste("SELECT DISTINCT detections.* FROM detections JOIN bins_detections ON bins_detections.detections_id = detections.id JOIN bins ON bins.id = bins_detections.bins_id JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON bins_effort.effort_id = effort.id WHERE effort.name = '",fgname,"' AND status = 1 AND procedure = ",procedure," AND signal_code =",signal_code,sep="")))
  FG = dbFetch(dbSendQuery(conn,paste("SELECT bins.*,soundfiles.datetime FROM bins JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON bins_effort.effort_id = effort.id JOIN soundfiles ON bins.soundfiles_id = soundfiles.id WHERE effort.name = '",fgname,"'",sep="")))

  #delete any negatives that are already in there
  det_negs = dets[which(dets$label==0),]
  det_negs_id = as.integer(det_negs$id)

  dets = dets[which(dets$label!=0),]

  if(is.null(high_freq)){

    #first, see if available through lookup.

    high_freq = dbFetch(dbSendQuery(conn,paste("SELECT visible_hz FROM procedures WHERE id = ",procedure,sep="")))$visible_hz

    if(is.null(high_freq) | is.na(high_freq)){

      #if not available through lookup, use max existing detection
      high_freq = dbFetch(dbSendQuery(conn,paste("SELECT MAX(high_freq) FROM detections WHERE procedure = ",procedure," AND signal_code = ",signal_code,sep="")))$max

      if(is.null(high_freq) | is.na(high_freq)){

        stop("no way to determine high_freq from info on db. Please specify a value into function argument 'high_freq'")

      }

    }

  }

  i_neg_out = i_neg_interpolate(dets,FG,high_freq,procedure,signal_code,2)

  #make modified date be most recent modified positive.
  if(nrow(dets)>0){
    i_neg_out$modified = max(dets$modified)
  }else{
    #else, make it default to now.
    i_neg_out$modified = NULL
  }

  #compare i_neg_out to existing negatives from db. if all the important columns are the same, don't upload it. delete and reupload
  #for any differences.

  i_neg_out$start_file = as.integer(i_neg_out$start_file)
  i_neg_out$end_file = as.integer(i_neg_out$end_file)

  i_neg_out$id = NA
  i_neg_out$temp_id = 1:nrow(i_neg_out)

  if(nrow(det_negs) > 0){

    det_negs$id = as.integer(det_negs$id)
    det_negs$temp_id = NA

    det_negs$start_file = as.integer(det_negs$start_file)
    det_negs$end_file = as.integer(det_negs$end_file)

    combine_negs = rbind(i_neg_out[,c("id","temp_id","start_time","end_time","low_freq","high_freq","start_file","end_file","procedure","label","signal_code")],
                         det_negs[,c("id","temp_id","start_time","end_time","low_freq","high_freq","start_file","end_file","procedure","label","signal_code")])

    combine_negs$duplicated = duplicated(combine_negs[c(3:length(combine_negs))]) | duplicated(combine_negs[c(3:length(combine_negs))],fromLast = TRUE)
    db_to_del = combine_negs$id[-which(combine_negs$duplicated)]
    db_to_del = db_to_del[-which(is.na(db_to_del))]

    #delete on db if applicable
    if(length(db_to_del)>0){

      print("deleting existing negatives which now conflict...")
      #don't hard delete since it will delete all dets even if procedure changed
      table_delete(conn,'detections',db_to_del)

      det_negs2 = det_negs[which(det_negs$id %in% db_to_del),]

      #only delete from same procedure and signal code
      dets_left = dbFetch(dbSendQuery(conn,paste("SELECT detections.* FROM detections WHERE original_id IN (",paste(as.integer(det_negs2$original_id),collapse=",",sep=""),") AND label = 0 AND procedure = ",procedure," AND status = 2 AND signal_code = ",signal_code,sep="")))

      dets_left_id = as.integer(dets_left$id)
      #print(dets_left_id)
      if(length(dets_left_id)>0){
        table_delete(conn,'detections',dets_left_id)
      }
    }

    local_to_push = combine_negs$temp_id[-which(combine_negs$duplicated)]
    local_to_push = local_to_push[-which(is.na(local_to_push))]
  }else{

    local_to_push = i_neg_out$temp_id

  }

  if(length(local_to_push)>0){

    i_neg_out = i_neg_out[which(i_neg_out$temp_id %in% local_to_push),]
    i_neg_out$temp_id=NULL
    i_neg_out$id = NULL

    print(paste("submitting i_neg data for effort name:",fgname,", procedure:",procedure,"and signal_code:",signal_code))

    out = dbAppendTable(conn,"detections",i_neg_out)

    print("data submitted!")

    return(out)

  }else{

    print("no changes detected in positive data, leaving unchanged.")

    return(0)
  }



}

#' Create assumed detections for implied negative data
#'
#' Create assumed detections from ground truth detections.
#' @param data The dataset. Uses start and end time and start and end file. column names need to be in db format. must correspond with fg.
#' @param FG The filegroup, in db format (bins.soundfiles_id, bins.seg_start,bins.seg_end, soundfiles.datetime)
#' @param high_freq high frequency of the implied negative detection.
#' @param procedure procedure id for the implied negative detections
#' @param signal_code signal id for the implied negative detections
#' @param analyst analyst id for the implied negative detections
#' @return dataframe in db format of implied negative detections
#' @export
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

    dets= data[which((data$start_file == FG$soundfiles_id[i] & (data$start_time>= FG$seg_start[i] & data$start_time < FG$seg_end[i]))
                     | (data$end_file == FG$soundfiles_id[i] & (data$end_time<= FG$seg_end[i] & data$end_time > FG$seg_start[i]) )),]

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

    #deal with overlapping dets- if there are consecutive starts/ends, take the min of start and the max of end

    if(nrow(times)>1){
      del = c()
      for(p in 2:nrow(times))
        if(times[p-1,"meaning"]==times[p,"meaning"]){
          if(times[p,"meaning"] == "start"){
            del = c(del,p)
          }else{
            del = c(del,p-1)
          }

        }
    }

    if(length(del)>0){
      times = times[-del,]
    }


    for(p in 1:nrow(times)){

      if(times$meaning[p]=='end'){
        start_time = max(start_time,times$time[p]) #this will chose the latest possible start time in case of multiple ends overlapping.
      }else{
        detsout[[counter]]=c(start_time,times$time[p])
        counter = counter + 1
      }

    }

    detsout = do.call("rbind",detsout)

    #if(as.integer(FG$soundfiles_id[i])==4171013){
    #  print(dets)
    #  print(detsout)
    #}


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

#' convert a dataset from detx format to db format
#'
#' convert a dataset from detx format to db format. Could use work to be a little more general in cases where detx has variable fields.
#' @param conn The database connection
#' @param detx_data The dataset in detx format
#' @param procedure procedure id for the output detections (no equivalent in detx)
#' @param strength strength_codes id for the output detections (no equivalent in detx)
#' @param depnames additional column to provide in case of file name abiguity. Must match to values in data_collection 'name' column.
#' @return a db format data frame
#' @export
detx_pgpamdb_det_rough_convert<-function(conn,detx_data,procedure,strength,depnames= NULL){


  #determine these values through matches
  label_lookup= lookup_from_match(conn,'label_codes',unique(detx_data$label),'alias')
  labels = label_lookup$id[match(detx_data$label,label_lookup$alias)]
  sc_lookup= lookup_from_match(conn,'signals',unique(detx_data$SignalCode),'code')
  signal_codes = sc_lookup$id[match(detx_data$SignalCode,sc_lookup$code)]
  la_lookup = lookup_from_match(conn,'personnel',unique(detx_data$LastAnalyst),'code')
  last_analyst = la_lookup$id[match(detx_data$LastAnalyst,la_lookup$code)]

  startind = which(colnames(detx_data)=='StartTime')

  outdata =data.frame(detx_data[,c((startind):(5+startind),which(colnames(detx_data)=='probs'),max(which(colnames(detx_data)=='comments'),which(colnames(detx_data)=='Comments')))],procedure,labels,signal_codes,strength,last_analyst)

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
    file_lookup = lookup_from_match(conn,"soundfiles",unique(c(outdata$start_file,outdata$end_file)),"name")
  }
  outdata$start_file = file_lookup$id[match(outdata$start_file,file_lookup$name)]
  outdata$end_file = file_lookup$id[match(outdata$end_file,file_lookup$name)]


  outdata[which(is.na(outdata$comments)),"comments"]=""

  return(outdata)

}

#DML and SQL operations.

#' Query with table of values
#'
#' Query a user selected table or join of tables, using a dataset from R session. interpret as maps column names of R dataset, in
#' order, to names of equivalent columns on database.
#' @param conn The database connection
#' @param from_db_query The query, including select and joins, which will be joined to user R dataset.
#' @param dataset The R object to update, all columns must have equivalents in database table. Defaults to column names.
#' @param data_types The postgresql target column data types. Must be provided and explicit.
#' @param interpret_as Ordered vector which maps dataset columns to database (including join syntax, if required) columns
#' @param return_anything If the output df is different sized than the input, return it anyway, otherwise return error
#' @return a data frame corresponding to returned columns specified in 'from_db_query' argument
#' @export
table_dataset_lookup<-function(conn,from_db_query,dataset,data_types,interpret_as=NULL,return_anything=FALSE){

  #SELECT * FROM answers
  #JOIN (VALUES (4509, 'B'), (622, 'C'), (1066, 'D'), (4059, 'A'), (4740, 'A'))
  #AS t (p,o)
  #ON p = problem_id AND o = option

  #
  #from_db_query = "SELECT * FROM bins JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id"
  #dataset = format_fg_query
  #data_types = c("timestamp","character varying","DOUBLE PRECISION","DOUBLE PRECISION")

  if(!is.null(interpret_as)){
    colnames(dataset) = interpret_as
  }

  #convert posixct to character:
  for(i in 1:length(dataset)){
    if(class(dataset[,i])[1] == "POSIXct"){
      dataset[,i] = format(dataset[,i], format = "%Y-%m-%d %H:%M:%OS%z")
    }
  }

  #
  #from_db_query = "SELECT * FROM bins JOIN soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id"

  query1 = " JOIN (VALUES "

  query3 = paste(") AS t (",paste(letters[1:length(dataset)],collapse=",",sep=""),") ON ",paste(paste(letters[1:length(dataset)],"=",colnames(dataset),sep=" "),collapse=" AND "),sep="")

  #single transaction.
  dbBegin(conn)

  try({
   #get data types
   #dtypes = not sure if need

   q2 = ""
   ds_ser=as.vector(t(unlist(dataset)))
   #generate sequence:
   vals=list()
   for(i in 1:nrow(dataset)){
     vals[[i]] = seq(from = i, by = nrow(dataset),length.out = length(dataset))
   }
   vals=do.call("c",vals)

   ds_by_row = ds_ser[vals]
   item_count = 0
   start_ind = 1
   chunk = 1
   segs = list()
   for(i in 1:nrow(dataset)){
     q2_sub = "("
     for(j in 1:length(dataset)){
       item_count = item_count+1
       new_val = paste("$",item_count,"::",data_types[j],sep="")
       if(j !=length(dataset)){
         new_val=paste(new_val,",",sep="")
       }else{
         new_val=paste(new_val,")",sep="")
       }
       q2_sub= paste(q2_sub,new_val,sep="")
     }

     if(i ==nrow(dataset) | object.size(q2)> 75000){

       q2 = paste(q2,q2_sub,sep="")

       #query = paste("INSERT INTO",tablename,q1,"VALUES",q2,"returning",idname,";")
       query =paste(from_db_query,query1,q2,query3,sep="")

       segs[[chunk]] = dbFetch(dbBind(dbSendQuery(conn, query), params=ds_by_row[start_ind:(i*length(dataset))]))

       start_ind = (i*length(dataset)) + 1

       item_count = 0

       chunk = chunk+1

       q2 = ""

     }else{
       q2=paste(q2,q2_sub,",",sep="")

     }

   }

  })

  dbCommit(conn)

  segs = do.call("rbind",segs)

  #check that nrow is the same

  if(return_anything==FALSE){
    if(nrow(segs)!=nrow(dataset)){

      stop("Returned dataset not same nrow as query dataset")

    }

  }

  return(segs)

}

#' Insert data into the database
#'
#' Insert data into a named database table. Assumes postgresql db with existing connection.
#' @param conn The database connection
#' @param tablename The name of the data table in the database
#' @param dataset The R object to update- must have id column matching type of tablename, and all columns must have equivalents in database table.
#' @param colvector Vector which specifies specific columns from dataset to update: otherwise assumes all matching column names from dataset to be updated
#' @param idname character string specifying name of primary key.
#' @return a data frame of the ids of the uploaded dataset, in order, from the target table.
#' @export
table_insert <-function(conn,tablename,dataset,colvector=NULL,idname = 'id'){

  #sanitize dataset for integer 64 type, and convert back to integer (doesn't work with dbind further below)
  for(i in 1:length(dataset)){
    if(class(dataset[,i])=="integer64"){
      dataset[,i]=as.integer(dataset[,i])
    }
  }

  if(is.null(colvector)){
    colvector=colnames(dataset)
  }else{
    #truncate dataset to just colvector and id
    dataset = dataset[,which(colnames(dataset) %in% colvector)]

  }

  #check that dataset has all required cols.

  if(any(!colvector %in% colnames(dataset))){
    stop("insuffecient columns provided in dataset argument")
  }

  cols = paste("('",paste(colvector,collapse="','"),"')",sep="")

  dtquery = paste("SELECT data_type,column_name
               FROM information_schema.columns
               WHERE table_schema = 'public'
                  AND table_name ='",tablename,"'
                  AND column_name IN ",cols,sep="")

  dtquery <- gsub("[\r\n]", " ", dtquery)

  #single transaction.
  dbBegin(conn)

  try({
  #get data types
  dtypes = dbFetch(dbSendQuery(conn,dtquery))

  #check data types match.
  if(any(!colvector %in% dtypes$column_name)){
    stop("columns do not match database table names")
  }

  #serialize dataset and construct query.
  #first part of query
  q1 = paste("(",paste(colvector,collapse=",",sep=""),")",sep="")

  #second part of query
  #need to chunk into 100KB size to avoid something on the dbi backend rejecting query size.
  q2 = ""
  ds_ser=as.vector(t(unlist(dataset)))
  #generate sequence:
  vals=list()
  for(i in 1:nrow(dataset)){
    vals[[i]] = seq(from = i, by = nrow(dataset),length.out = length(colvector))
  }
  vals=do.call("c",vals)

  ds_by_row = ds_ser[vals]
  item_count = 0
  start_ind = 1
  chunk = 1
  ids = list()
  for(i in 1:nrow(dataset)){
    q2_sub = "("
    for(j in 1:length(colvector)){
      item_count = item_count+1
      new_val = paste("$",item_count,"::",dtypes[which(dtypes$column_name == colvector[j]),"data_type"],sep="")
      if(j !=length(colvector)){
        new_val=paste(new_val,",",sep="")
      }else{
        new_val=paste(new_val,")",sep="")
      }
      q2_sub= paste(q2_sub,new_val,sep="")
    }

    if(i ==nrow(dataset) | object.size(q2)> 75000){

      q2 = paste(q2,q2_sub,sep="")

      query = paste("INSERT INTO",tablename,q1,"VALUES",q2,"returning",idname,";")

      ids[[chunk]] = dbFetch(dbBind(dbSendQuery(conn, query), params=ds_by_row[start_ind:(i*length(colvector))]))

      start_ind = (i*length(colvector)) + 1

      item_count = 0

      chunk = chunk+1

      q2 = ""

    }else{
      q2=paste(q2,q2_sub,",",sep="")

    }

  }

  })

  dbCommit(conn)

  ids = do.call('rbind',ids)

  return(ids)

}


#' Update data into the database
#'
#' Update data into a named database table. Assumes postgresql db with existing connection.
#' @param conn The database connection
#' @param tablename The name of the data table in the database
#' @param dataset The R object to update- must have id column matching type of tablename, and all columns must have equivalents in database table.
#' @param colvector Vector which specifies specific columns from dataset to update: otherwise assumes all matching column names from dataset to be updated
#' @param idname character string specifying name of primary key.
#' @return result
#' @export
table_update <-function(conn,tablename,dataset,colvector=NULL,idname = 'id'){

  idname= as.character(idname)

  #if id is not first column of dataset, make it be
  if(colnames(dataset)[1]!=idname){
    dataset=data.frame(dataset[[idname]],dataset[,which(colnames(dataset)!=idname)])
    colnames(dataset)[1]=idname
  }

  #sanitize dataset for integer 64 type, and convert back to integer (doesn't work with dbind further below)
  for(i in 1:length(dataset)){
    if(class(dataset[,i])=="integer64"){
      dataset[,i]=as.integer(dataset[,i])
    }
  }

  if(is.null(colvector)){
    colvector_wid=colnames(dataset)
    colvector = colvector_wid[-which(colvector_wid==idname)]
  }else{

    colvector_wid = c(idname,colvector)
    colvector_wid=colvector_wid[which(!duplicated(colvector_wid))]

    #truncate dataset to just colvector and id
    dataset = dataset[,which(colnames(dataset) %in% colvector_wid)]

  }

  #check that dataset has all required cols.

  if(any(!colvector_wid %in% colnames(dataset))){
    stop("insuffecient columns provided in dataset argument")
  }



  #check that only integer ids are passed
  if(!all(is.finite(as.integer(dataset[[idname]])))){
    stop("invalid id values passed in dataset")
  }

  dataset[[idname]] = as.integer(dataset[[idname]])

  cols = paste("('",paste(colvector_wid,collapse="','"),"')",sep="")

  dtquery = paste("SELECT data_type,column_name
               FROM information_schema.columns
               WHERE table_schema = 'public'
                  AND table_name ='",tablename,"'
                  AND column_name IN ",cols,sep="")

  dtquery <- gsub("[\r\n]", " ", dtquery)

  #single transaction.
  dbBegin(conn)

  try({

  #get data types
  dtypes = dbFetch(dbSendQuery(conn,dtquery))

  #check data types match.
  if(any(!colvector %in% dtypes$column_name)){
    stop("columns do not match database table names")
  }

  #query the data table to be modified. Compare to the existing dataset, and reduce it
  #to only the modified data.

  existingdata = dbFetch(dbSendQuery(conn,paste("SELECT",paste(colvector_wid,collapse = ",",sep=""),"FROM",
                                        tablename,"WHERE",idname,"IN (",paste(dataset$id,collapse=","),");")))

  for(i in 1:length(existingdata)){
    if(class(existingdata[,i])=="integer64"){
      existingdata[,i]=as.integer(existingdata[,i])
    }
  }

  comb_data <- merge(existingdata, dataset,by=colvector_wid, all=TRUE)

  delta_ids = comb_data$id[which(duplicated(comb_data$id))]

  if(length(delta_ids)>0 & length(delta_ids)<nrow(dataset)){

    warning("Some identical rows to database copy provided in dataset. Only updating changed rows.")

    dataset = dataset[which(dataset$id %in% delta_ids),]

  }else if(length(delta_ids)==0){

    stop("Error: no changes detected in the provided dataset compared to database copy. Terminating")
  }

  #serialize dataset and construct query.
  #first part of query
  q1 = ""
  tempnamesvec = c()
  for(i in 1:length(colvector)){
    tempnamesvec = c(tempnamesvec,paste("c",i,sep=""))
    q1_sub= paste(colvector[i],paste("temp",tempnamesvec[i],sep="."),sep="=")
    if(i !=length(colvector)){
      q1_sub=paste(q1_sub,",",sep="")
    }

    q1 = paste(q1,q1_sub,sep="")
  }

  #third part of query
  q3 = paste("temp(",idname,",",paste(tempnamesvec,collapse = ","),")",sep="")

  #second part of query
  q2 = ""
  ds_ser=as.vector(t(unlist(dataset)))
  #generate sequence:
  vals=list()
  for(i in 1:nrow(dataset)){
    vals[[i]] = seq(from = i, by = nrow(dataset),length.out = length(colvector_wid))
  }
  vals=do.call("c",vals)

  ds_by_row = ds_ser[vals]
  item_count = 0
  start_ind = 1

  for(i in 1:nrow(dataset)){

    q2_sub = "("
    for(j in 1:length(colvector_wid)){
      item_count = item_count+1
      new_val = paste("$",item_count,"::",dtypes[which(dtypes$column_name == colvector_wid[j]),"data_type"],sep="")
      if(j !=length(colvector_wid)){
        new_val=paste(new_val,",",sep="")
      }else{
        new_val=paste(new_val,")",sep="")
      }
      q2_sub= paste(q2_sub,new_val,sep="")
    }

    if(i ==nrow(dataset) | object.size(q2)> 75000){

      q2 = paste(q2,q2_sub,sep="")

      query = paste("UPDATE",tablename,"AS m SET",q1,"FROM (values",q2,") AS",q3,"WHERE",paste("m.",idname,sep=""),"=",paste("temp.",idname,sep=""))

      dbBind(dbSendQuery(conn, query), params=ds_by_row[start_ind:(i*length(colvector_wid))])

      start_ind = (i*length(colvector_wid)) + 1

      item_count = 0

      q2 = ""

    }else{
      q2=paste(q2,q2_sub,",",sep="")

    }

  }

  })

  dbCommit(conn)

}

#' Delete data in the database by id
#'
#' Delete data in a named database table. Assumes postgresql db with existing connection.
#' @param conn The database connection
#' @param tablename The name of the data table in the database
#' @param ids The id vector to delete. All rows with id in vector will be deleted.
#' @param idname character string specifying name of primary key.
#' @param hard_delete bool specifying whether to 'hard delete', refering to the archiving behavior of detections table on first delete. Only does anything if tablename = 'detections'
#' @return result
#' @export
table_delete <-function(conn,tablename,ids,idname = 'id',hard_delete=FALSE){

  dbBegin(conn)

  try({

  if(length(ids)>1){
    #query = paste("DELETE FROM ",tablename," WHERE ",idname," =ANY(Array [",paste(ids,collapse=",",sep=""),"])",sep="")

    query = paste("DELETE FROM ",tablename," WHERE ",idname," IN (",paste(ids,collapse=",",sep=""),")",sep="")
  }else{
    query = paste("DELETE FROM ",tablename," WHERE ",idname," IN",paste("(",ids,")",sep=""))

  }

  if(tablename =="detections" & hard_delete==TRUE){

    tablength = length(ids)

    start_curr = dbFetch(dbSendQuery(conn,"SELECT last_value FROM detections_id_seq"))+1

    start_curr = as.integer(start_curr$last_value)

    ids_above_currval = dbFetch(dbSendQuery(conn,paste("SELECT id FROM detections WHERE id >",start_curr,"-1")))

    #execute query first time here.
    dbExecute(conn,query)

    end_curr = start_curr+length(ids)-1
    new_ids = start_curr:end_curr

    #check if there are any ids above currval in the new sequence. If there are, requires more elaborate calculation.
    #if not, can proceed with the assumption that these new ids can be deleted.
    if(length(ids_above_currval$id)>0){
      ids_above_currval$id = as.integer(ids_above_currval$id)
      if(any(ids_above_currval %in% new_ids)){
        stop("Cannot assume new ids due to the presence of manually inserted ids above detections id sequence. Transaction aborted.")
      }
    }

    if(length(ids)>1){
      #query = paste("DELETE FROM ",tablename," WHERE original_id =ANY(Array [",paste(ids,collapse=",",sep=""),"])",sep="")
      query = paste("DELETE FROM ",tablename," WHERE id IN (",paste(new_ids,collapse=",",sep=""),")",sep="")
    }else{
      query = paste("DELETE FROM ",tablename," WHERE id IN",paste("(",new_ids,")",sep=""))

    }

    out = dbExecute(conn,query)

  }

  })

  dbCommit(conn)

  return(out)

}


#convenience functions.

#' Look up database table by named column values and return id

#'
#'
#' Look up database table by named column values and return id. Useful for data conversions. Assumes postgresql db with existing connection.
#' @param conn The database connection
#' @param tablename The name of the data table in the database
#' @param vector The values to compare with database table.
#' @param match_col The name of the database column to compare the vector columns to
#' @param idname character string specifying name of primary key.
#' @return lookup table of ids and the initial vector values
#' @export
#'
lookup_from_match <- function(conn,tablename,vector,match_col,idname='id'){

  #build in chunks of size which will fit
  #size_limit = 332498 #approximate, large but safe
  #size_limit = 250000
  size_limit = 50000

  id_and_name = paste(idname,match_col,sep=",")


  dbBegin(conn)

  try({


  chunks = as.integer(ceiling(object.size(vector) / size_limit))

  approx_len = ceiling(length(vector)/chunks)

  chunks_out = list()

  for(i in 1:chunks){

    end_in = (approx_len*i)

    if(end_in>length(vector)){
      end_in= length(vector)
    }

    vector_in= vector[((approx_len*(i-1))+1):end_in]

    if(length(vector_in)>1){
      sf_id_lookup = paste("SELECT ",id_and_name," FROM ",tablename," WHERE ",match_col," IN",paste("(",paste("$",1:(length(vector_in)-1),collapse=",",sep=""),",$",length(vector_in),")",sep=""))
    }else{
      sf_id_lookup = paste("SELECT ",id_and_name," FROM ",tablename," WHERE ",match_col," IN",paste("($",length(vector_in),")",sep=""))
    }

    query1 <- dbSendQuery(conn, sf_id_lookup)
    dbBind(query1, params=vector_in)

    res = dbFetch(query1)

    dbClearResult(query1)

    res[[idname]]=as.integer(res[[idname]])

    chunks_out[[i]] = res

  }

  chunks_out = do.call("rbind",chunks_out)

  })

  dbCommit(conn)

  #if(any(duplicated(chunks_out))){

  #  chunks_out = chunks_out[-duplicated(chunks_out),]
  #}

  return(chunks_out)

}

vector_validate = function(conn,table,column,vector){

  #very simple fxn to determine if vector exists in column.

  if(is.numeric(vector[1])){
    spacer = ""
  }else{
    spacer = "'"
  }

  query = paste("SELECT ",column," FROM ",table," WHERE ",column," IN ","(",spacer,paste(vector,collapse=paste(spacer,",",spacer,sep=""),sep=""),spacer,")",sep="")

  out = dbFetch(dbSendQuery(conn,query))

  if(length(vector[-which(vector %in% out$name)])==0){

    print('All vector elements found in column')
    out = NULL
  }else{
    print('Some vector elements not found')
    out =vector[-which(vector %in% out$name)]

  }

  return(out)

}

#' read wav files on NAS of a specific mooring and upload them to db.

#'
#'
#' Read a mooring deployment soundfiles from sound file collection and load metadata to db Assumes postgresql db with existing connection.
#' @param conn The database connection
#' @param rootpath The path of the sound file collection. Assumes mooring_name/mm_yyyy/{filename}
#' @param mooring_name The mooring name.
#' @return result
#' @export
#'
load_soundfile_metadata<-function(conn,rootpath,mooring_name){

  #needs foreach and tuneR

  print(paste("Reading rootpath for soundfile metadata...",Sys.time()))

  path1 = paste(rootpath,mooring_name,sep="/")
  m_ys = dir(path1)
  subdirs = foreach(i=1:length(m_ys)) %do% {
    path2 = paste(path1,m_ys[i],sep="/")
    files= dir(path2)
    filesout = foreach(z=1:length(files)) %do% {
      path3 = paste(path2,files[z],sep="/")
      header = readWave(path3,header = TRUE)
      return(c(files[z],paste("/",mooring_name,"/",m_ys[i],"/",sep=""),round(header$samples/header$sample.rate,2),mooring_name))
    }

    filesout = do.call("rbind",filesout)
    return(filesout)
  }

  subdirs_ = do.call("rbind",subdirs)

  dateinfo = substr(subdirs_[,1],nchar(subdirs_[,1])-16,nchar(subdirs_[,1])-4)
  dateinfo=as.POSIXlt(dateinfo,format="%y%m%d-%H%M%S",tz="UTC")

  target_mooring= dbFetch(dbSendQuery(conn,paste("SELECT id FROM data_collection WHERE name = '",mooring_name,"'",sep="")))

  dataout = data.frame(as.integer(target_mooring$id),dateinfo,subdirs_[,3],subdirs_[,1])
  colnames(dataout) = c("data_collection_id","datetime","duration","name")

  print(paste("Submitting to database...",Sys.time()))

  rs = dbAppendTable(conn,"soundfiles" , dataout)

  return(rs)

}

#' Convert between INSTINCT detx detection format and database detection format
#'
#' Convert between INSTINCT detx detection format and database detection format. Assumes postgresql db with existing connection.
#' @param conn The database connection
#' @param dataset The R detx object.
#' @return result
#' @export
#'
detx_to_db <- function(conn,dataset){

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
  sf_ids = lookup_from_match(conn,"soundfiles",unique(c(dataset$StartFile,dataset$EndFile)),"name")
  dataset$StartFile = sf_ids$id[match(dataset$StartFile,sf_ids$name)]
  dataset$EndFile = sf_ids$id[match(dataset$EndFile,sf_ids$name)]

  #ids for label
  lab_id = lookup_from_match(conn,"label_codes",dataset$label,"alias")
  dataset$label = lab_id$id[match(dataset$label,lab_id$alias)]

  #ids for signal_code
  sigcode_id = lookup_from_match(conn,"signals",dataset$SignalCode,"code")
  dataset$SignalCode = sigcode_id$id[match(dataset$SignalCode,sigcode_id$code)]

  #ids for signal_code
  strength_id = lookup_from_match(conn,"strength_codes",dataset$strength,"name")
  dataset$strength = strength_id$id[match(dataset$strength,strength_id$name)]

  if("analyst" %in% colnames(dataset)){

    #ids for lastanalyst
    pers_id = lookup_from_match(conn,"personnel",dataset$analyst,"code")
    dataset$analyst = pers_id$id[match(dataset$analyst,pers_id$code)]

  }


  dataset$VisibleHz=NULL
  dataset$LastAnalyst=NULL
  dataset$Type= NULL
  dataset$id = NULL

  colnames(dataset)[match(c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","Comments","probs","SignalCode"),
                          colnames(dataset))]=c("start_time","end_time","low_freq","high_freq","start_file","end_file","comments","probability","signal_code")

  dataset$comments[which(is.na(dataset$comments))]=""

  return(dataset)

}

#' Plot all instances of a label code
#'
#' Plot all instances of a label code. Shows spread of label across data regardless of analysis. Gray signifies data present, no
#' effort, boxes around tile indicate at least one positive presence instance.
#' @param conn The database connection
#' @param bin_label bin label code, ie 'RW'
#' @param inst_source AFSC if you don't want to include other lab data in the db
#' @param plot_sds standard deviations for the color scale. Smaller = more similar colors, larger = more dynamic colors.
#' @param bin_type integer id for bin type- 1 = LOW, 2 = REG, 3 = SHI. Affects n but shouldn't affect plot too much.
#' @return ggplot object
#' @export
#'
bin_label_explore<-function(conn,bin_label,inst_source='AFSC',plot_sds = 4,bin_type=1){

  bt_str = c("LOW","REG","SHI","CUSTOM")[bin_type]
  #bin_label= 'fw' #temp
  #inst_source = "AFSC"

  if(inst_source == "AFSC"){
    #bound the approximate region to get rid of the GR and other non-region AFSC data.
    long_min = -185
    long_max = -140
  }

  query = paste("SELECT ",bin_label,",count(*),date_trunc('month', soundfiles.datetime) AS dt_,data_collection.name,data_collection.location_code,data_collection.latitude
                FROM bin_label_wide JOIN bins ON bins.id = bin_label_wide.id JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
                WHERE bins.type = ",bin_type," AND data_collection.institution_source = '",inst_source,"' AND data_collection.longitude < ",long_max," AND data_collection.longitude > ",long_min,
                " GROUP BY ",bin_label,",dt_,data_collection.name,data_collection.location_code,data_collection.latitude",sep="")

  output = dbFetch(dbSendQuery(conn,gsub("[\r\n]", "", query)))

  #cool, we've got it

  #avg_lat for site
  avg_lat = aggregate(output$latitude, list(output$location_code), FUN=function(x) mean(x,na.rm=TRUE))
  colnames(avg_lat)=c("Site","Lat")

  #combine output table so that 1/21 and 0/20 are combined, and 99s are distinct

  output_mod = output

  output_mod$common_label= 0

  output_mod[which(output_mod[,bin_label]==21),"common_label"]=1
  output_mod[which(output_mod[,bin_label]==1),"common_label"]=1
  output_mod[which(output_mod[,bin_label]==0),"common_label"]=0
  output_mod[which(output_mod[,bin_label]==20),"common_label"]=0
  output_mod[which(output_mod[,bin_label]==99),"common_label"]=2

  output_mod$count= as.integer(output_mod$count)

  output_mod_agg =aggregate(output_mod$count, list(output_mod$dt,output_mod$name,output_mod$location_code,output_mod$latitude,output_mod$common_label), FUN=function(x) sum(x,na.rm=TRUE))

  colnames(output_mod_agg)= c(colnames(output_mod[3:length(output_mod)]),"count")

  output_mod_agg$count[which(output_mod_agg$common_label==2)]=NA

  output_mod_agg$avg_lat = avg_lat[match(output_mod_agg$location_code,avg_lat$Site),"Lat"]

  output_mod_agg$dt_num = as.numeric(output_mod_agg$dt_)
  output_mod_agg$avg_lat_chr = as.character(round(output_mod_agg$avg_lat,2))


  output_mod_agg$location_code =factor(output_mod_agg$location_code,level = unique(output_mod_agg$location_code[order(output_mod_agg$avg_lat_chr)]))

  output_mod_agg$sub_na = FALSE

  output_mod_agg[which(!is.na(output_mod_agg$count)),"sub_na"] = (paste(output_mod_agg[which(!is.na(output_mod_agg$count)),"dt_"],
                                                                        output_mod_agg[which(!is.na(output_mod_agg$count)),"location_code"]) %in%
                                                                    paste(output_mod_agg[which(is.na(output_mod_agg$count)),"dt_"],
                                                                          output_mod_agg[which(is.na(output_mod_agg$count)),"location_code"]))

  output_mod_agg_sub = output_mod_agg[which(output_mod_agg$sub_na),]

  output_mod_agg = output_mod_agg[-which((paste(output_mod_agg$dt_,output_mod_agg$location_code) %in%
                                            paste(output_mod_agg_sub$dt_,output_mod_agg_sub$location_code)) &
                                           is.na(output_mod_agg$count)),]

  output_mod_agg$count_adj = output_mod_agg$count* output_mod_agg$common_label

  month_lookup = aggregate(output_mod_agg$count,list(output_mod_agg$dt_,output_mod_agg$name),sum)
  colnames(month_lookup) = c("dt","name","total_bins")

  output_mod_agg$count_adj = output_mod_agg$count_adj/ month_lookup$total_bins[match(paste(output_mod_agg$dt_,output_mod_agg$name),paste(month_lookup$dt,month_lookup$name))]

  scale_midpoint = mean(output_mod_agg$count_adj,na.rm=TRUE)
  scale_sd = sd(output_mod_agg$count_adj,na.rm=TRUE)

  scale_max = scale_midpoint+ scale_sd*plot_sds

  if(scale_max>1){
    scale_max = 1
  }

  output_mod_agg[which(output_mod_agg$count_adj>scale_max),"count_adj"]=scale_max

  colnames(output_mod_agg)[which(colnames(output_mod_agg)=="dt_")]="mm_yy"

  base_map =ggplot(output_mod_agg, aes(mm_yy, location_code)) +
    geom_tile(aes(fill = count_adj), color = "gray") +
    geom_tile(data = output_mod_agg[which(output_mod_agg$count>0 & output_mod_agg$common_label==1),], color = "black",fill=NA) +
    scale_fill_gradient2(limits=c(0, scale_max), low = "white", high= "purple",
                         midpoint =  scale_midpoint) +
    #scale_x_discrete(expand = c(0,0)) +
    scale_x_datetime(date_breaks = "6 months" , date_labels = "%m-%y",expand=c(0,0)) +
    ggtitle(paste(bin_label,"monthly",bt_str,"bin % presence")) +
    theme_bw() +
    theme(legend.title = element_text(size=12),
          legend.text = element_text(size=12)) +
    theme(axis.text = element_text(size=12),
          axis.title.y = element_blank()) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
    labs(fill = "% presence")


  return(base_map)

}

#' Add a layer to bin label explore plot
#'
#' Add a layer to a bin label explore plot. No easy way to include legend, use for quick comparisons.
#' @param conn The database connection
#' @param signal_code integer id for signal, ie 1 (RW), 2 (GS). see db
#' @param procedure integer id for procedure. see db
#' @param color color. doesn't scale with counts, just presence.
#' @param fgnames optional fgs to consider.
#' @param inst_source AFSC if you don't want to include other lab data in the db
#' @return ggplot layer to + to bin label explore ggplot object
#' @export
#'
add_layer_ble<-function(conn,signal_code,procedure,color,fgnames=c(),inst_source='AFSC'){

  if(length(fgnames)>0){
    fgnames_join = "JOIN bins_detections ON detections.id = bins_detections.detections_id JOIN bins ON bins.id = bins_detections.bins_id JOIN bins_effort ON bins_effort.bins_id = bins.id JOIN effort ON effort.id = bins_effort.effort_id"

    add_fgnames = paste("AND effort.name IN ('",paste(fgnames,collapse = "','",sep=""),"')",sep="")
  }else{
    fgnames_join = ""
    add_fgnames = ""
  }

  query_layer = paste("SELECT DISTINCT date_trunc('month', soundfiles.datetime) AS mm_yy,data_collection.name,data_collection.location_code,data_collection.latitude
  FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id ",fgnames_join,"
  WHERE detections.signal_code = ",signal_code," AND detections.procedure = ",procedure," AND data_collection.institution_source = '",inst_source,"' ",add_fgnames," GROUP BY mm_yy,data_collection.name,data_collection.location_code,data_collection.latitude",sep="")

  query_layer_out = dbFetch(dbSendQuery(conn,gsub("[\r\n]", "", query_layer)))

  return(geom_point(data=query_layer_out, aes(x=mm_yy, y=location_code,z=NULL), fill = color, colour = 'black',pch=21,size =3))

}


#' @import RPostgres
#' @import foreach
#' @import tuneR
#' @import utils
#' @import ggplot2
NULL

