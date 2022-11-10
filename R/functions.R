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
                   dbname = "poc",
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

#this will update the sql dataset using an R dataset. requires column names of R dataset to be identical.
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
                  AND table_name ='data_collection'
                  AND column_name IN",cols)

  dtquery <- gsub("[\r\n]", "", dtquery)

  #single transaction.
  dbBegin(conn)

  #get data types
  dtypes = dbFetch(dbSendQuery(conn,dtquery))

  #check data types match.
  if(any(!colvector %in% dtypes$column_name)){
    stop("columns do not match database table names")
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

  #second part of query
  q2 = ""
  vals = seq(from = 1, by = nrow(dataset),length.out = length(colvector_wid))
  for(i in 1:nrow(dataset)){
    q2_sub = "("
    for(j in 1:length(colvector_wid)){
      new_val = paste("$",vals[j],"::",dtypes[which(dtypes$column_name == colvector_wid[j]),"data_type"],sep="")
      if(j !=length(vals)){
        new_val=paste(new_val,",",sep="")
      }else{
        new_val=paste(new_val,")",sep="")
      }
      q2_sub= paste(q2_sub,new_val,sep="")
    }

    if(i !=nrow(dataset)){
      q2_sub=paste(q2_sub,",",sep="")
      vals = vals + 1
    }

    q2 = paste(q2,q2_sub,sep="")

  }

  #third part of query
  q3 = paste("temp(",idname,",",paste(tempnamesvec,collapse = ","),")",sep="")

  #assemble query
  query = paste("UPDATE",tablename,"AS m SET",q1,"FROM (values",q2,") AS",q3,"WHERE",paste("m.",idname,sep=""),"=",paste("temp.",idname,sep=""))

  update <- dbSendQuery(conn, query)
  dbBind(update, params=as.vector(t(unlist(dataset))))

  dbCommit(conn)

}

#convenience functions.

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


#' @import RPostgres
#' @import foreach
#' @import tuneR
NULL

