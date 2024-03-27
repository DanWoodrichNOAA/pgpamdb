#this script will

#1. scan NAS for mooring names
#2. load mooring names from db (along with counts)
#3. for each mooring name with 0 counts on db, register mooring in d_c and load in the files from NAS

dbGet <-function(x){
  x = gsub("[\r\n]", "", x)
  dbFetch(dbSendQuery(con,x))
}
#here is the original script, reference to preserve rounding rules etc.

#modify columns for database.
library(foreach)
library(tuneR)
library(pgpamdb)
library(DBI)

source("./R/functions.R") #package under construction

source("./etc/paths.R") #populates connection paths which contain connection variables.

#as I use this script, I'm just deleting and changing code to create the lookup table. to upload fgs only, I only use the first
#part of the loop below

con=pamdbConnect("poc_v3",keyscript,clientkey,clientcert)

nas_moor_names= dir("//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves")
db_moor_names = dbGet("SELECT name FROM data_collection")
db_moor_names = db_moor_names$name

#looks like all of the nas_moor_names are currently on the db.
#all(nas_moor_names %in% db_moor_names)

#so need to assess a few things.
#are there db moorings that do not match nas moorings? (if so to add name to db table)
if(!all(nas_moor_names %in% db_moor_names)){
  
  moors_to_pub = data.frame(nas_moor_names[-which(nas_moor_names %in% db_moor_names)])
  
  colnames(moors_to_pub)="name"
  
  dbAppendTable(con,"data_collection",moors_to_pub)
  
}
#are there db moorings that are unpopulated? (if so, add registry of the files on nas to soundfiles)

moor_sfcounts = dbGet("SELECT COALESCE(COUNT(soundfiles.id), 0) as Count, data_collection.name as Name FROM data_collection LEFT JOIN soundfiles ON data_collection.id =
                      soundfiles.data_collection_id GROUP BY data_collection.name")

#for each mooring which contains zero sound files, upload from nas.

moor_to_upload = moor_sfcounts[which(moor_sfcounts$count==0),]

#limit to moorings on NAS
moor_to_upload = moor_to_upload[which(moor_to_upload$name %in% nas_moor_names),]

if(nrow(moor_to_upload)>0){
  
  SFs = foreach(n=1:length(moor_to_upload$name)) %do% {
    print(paste(moor_to_upload$name[n],n))
    #go to the NAS and start finding wavs. Need name, full path (for INSTINCT convenience, can be recalculated/assumed if need be), duration, deployment
    path1 = paste("//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves",moor_to_upload$name[n],sep="/")
    m_ys = dir(path1)
    subdirs = foreach(i=1:length(m_ys)) %do% {
      path2 = paste(path1,m_ys[i],sep="/")
      files= dir(path2,pattern=".wav")
      filesout = foreach(z=1:length(files)) %do% {
        path3 = paste(path2,files[z],sep="/")
        header = readWave(path3,header = TRUE)
        return(c(files[z],paste("/",moor_to_upload$name[n],"/",m_ys[i],"/",sep=""),round(header$samples/header$sample.rate,2),moor_to_upload$name[n]))
      }
      
      filesout = do.call("rbind",filesout)
      return(filesout)
    }
    subdirs = do.call("rbind",subdirs)
    
    SFs = data.frame(subdirs)
    
    SFs$datetime = as.POSIXct(substr(SFs[,1],nchar(SFs[,1])-16,nchar(SFs[,1])-4),format="%y%m%d-%H%M%S",tz='UTC')
    
    colnames(SFs) = c("name","path","duration","mooring_name","datetime")
    
    moor_name_lookup= lookup_from_match(con,"data_collection",unique(SFs$mooring_name),"name")
    
    SFs$data_collection_id = moor_name_lookup[match(SFs$mooring_name,moor_name_lookup$name),"id"]
    
    SFs$path=NULL
    SFs$mooring_name=NULL
    
    dbAppendTable(con,'soundfiles',SFs)
    
    print(paste("submitted",paste(moor_to_upload$name[n],n)))
  }

}
