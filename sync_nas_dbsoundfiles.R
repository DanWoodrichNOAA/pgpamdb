#this script will

#1. read the soundfiles available on nas
#2. compare with what it on database
#3. edit database to reflect truth data on NAS


#write it once I need to run the new stuff


#here is the original script, reference to preserve rounding rules etc.

#modify columns for database.
library(foreach)
library(tuneR)

#fxn from internet for all dups
isdup <- function(x){
  return(duplicated (x) | duplicated (x, fromLast = TRUE))
  }

setwd("C:/Users/daniel.woodrich/Desktop/database")
#switch back to LAN since not populating database while loading in GT- more secure here.
#setwd("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/database")

deployments <- read.csv("deployments.csv")

SFs = foreach(n=1:length(deployments$Name)) %in% {
  #go to the NAS and start finding wavs. Need name, full path (for INSTINCT convenience, can be recalculated/assumed if need be), duration, deployment
  path1 = paste("//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves",deployments$Name[n],sep="/")
  m_ys = dir(path1)
  subdirs = foreach(i=1:length(m_ys)) %do% {
    path2 = paste(path1,m_ys[i],sep="/")
    files= dir(path2)
    filesout = foreach(z=1:length(files)) %do% {
      path3 = paste(path2,files[z],sep="/")
      header = readWave(path3,header = TRUE)
      return(c(files[z],paste("/",deployments$Name[n],"/",m_ys[i],"/",sep=""),round(header$samples/header$sample.rate,2),deployments$Name[n]))
    }

    filesout = do.call("rbind",filesout)
    return(filesout)
  }
  subdirs = do.call("rbind",subdirs)
  return(subdirs)
}

SFs = do.call("rbind",SFs)
