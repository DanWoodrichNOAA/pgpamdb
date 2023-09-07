#dfo_2023_data format + trainin

#cut down on keystrokes
dbGet <-function(x){
  x = gsub("[\r\n]", "", x)
  dbFetch(dbSendQuery(con,x))
}

getmode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}


library(RPostgres)
library(tuneR)

#setwd("C:/Apps/pgpamdb")

source("./R/functions.R") #package under construction

source("./etc/paths.R") #populates connection paths which contain connection variables.

con=pamdbConnect("poc_v2",keyscript,clientkey,clientcert)

#load in metadata- load this in to dc

new_dfo_dc = read.csv("//nmfs/akc-nmml/CAEP/Acoustics/Projects/DFO RW/DFO 2023 data and analysis/DFO_NPRWdatasetMetadata_2021-03-25 wMMLnames_with_2023data.csv")

dc_cols = dbGet("SELECT * FROM data_collection LIMIT 0")

new_dfo_dc$start_data = as.POSIXct(new_dfo_dc$Data.StartDate,format = "%m/%d/%Y",tz="UTC")
new_dfo_dc$end_data = as.POSIXct(new_dfo_dc$Data.EndDate,format = "%m/%d/%Y",tz="UTC")

new_data_db = data.frame(new_dfo_dc$NewMooringName,new_dfo_dc$MooringSite,new_dfo_dc$Lat.DecDegrees,new_dfo_dc$Lon.DecDegrees,new_dfo_dc$start_data ,new_dfo_dc$end_data,NA,'DFO',new_dfo_dc$DeploymentID)
colnames(new_data_db) = c("name","location_code","latitude","longitude","start_data","end_data","sampling_rate","institution_source","oldid")

#add sampling rate:
path = "//161.55.120.117/NMML_AcousticsData/Working_Folders/DFO staging"
moorings = dir(path)

for(i in 1:nrow(new_data_db)){

  path2 = paste(path,moorings[grep(new_data_db$oldid[i],moorings)],sep="/")

  path3 = dir(path2)

  if(length(path3)==1){
    path4 = dir(paste(path2,path3,sep="/"),pattern=".wav")
    fullpath = paste(path2,path3,sep="/")
  }else{
    path4 = dir(path2,pattern=".wav")
    fullpath = path2
  }

  mode_sr= c()

  if(length(path4)<10){
    max_tries = length(path4)
  }else{
    max_tries = 10
  }

  for(f in 1:max_tries){

    metadata = readWave(paste(fullpath,path4[f],sep="/"),header=TRUE)
    mode_sr = c(mode_sr,metadata$sample.rate)
  }

  sr = getmode(mode_sr)

  new_data_db$sampling_rate[i] = sr
}

#dbAppendTable(con,"data_collection",new_data_db[,1:(length(new_data_db)-1)]) done

new_ids = table_dataset_lookup(con,"SELECT id,name FROM data_collection",new_data_db[,"name",drop=FALSE],c("character varying"))

#cool, now add to sfs to NAS, and db.
