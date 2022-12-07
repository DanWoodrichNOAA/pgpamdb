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


#DML and SQL operations.

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
#' @param colvector Vector which specifies specific columns from dataset to update: otherwise assumes all matching column names from dataset to be updated
#' @param idname character string specifying name of primary key.
#' @param hard_delete bool specifying whether to 'hard delete', refering to the archiving behavior of detections table on first delete. Only does anything if tablename = 'detections'
#' @return result
#' @export
table_delete <-function(conn,tablename,ids,idname = 'id',hard_delete=FALSE){

  if(length(ids)>1){
    #query = paste("DELETE FROM ",tablename," WHERE ",idname," =ANY(Array [",paste(ids,collapse=",",sep=""),"])",sep="")
    
    query = paste("DELETE FROM ",tablename," WHERE ",idname," IN (",paste(ids,collapse=",",sep=""),")",sep="")
  }else{
    query = paste("DELETE FROM ",tablename," WHERE ",idname," IN",paste("(",ids,")",sep=""))

  }

  if(tablename =="detections" & hard_delete==TRUE){

    #execute query first time here. Then modify it so all are deleted.
    dbExecute(conn,query)

    if(length(ids)>1){
      #query = paste("DELETE FROM ",tablename," WHERE original_id =ANY(Array [",paste(ids,collapse=",",sep=""),"])",sep="")
      query = paste("DELETE FROM ",tablename," WHERE original_id IN (",paste(ids,collapse=",",sep=""),")",sep="")
    }else{
      query = paste("DELETE FROM ",tablename," WHERE original_id IN",paste("(",ids,")",sep=""))

    }

  }

  return(dbExecute(conn,query))

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
    pers_id = lookup_from_match(con,"personnel",dataset$analyst,"code")
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



#' @import RPostgres
#' @import foreach
#' @import tuneR
NULL

