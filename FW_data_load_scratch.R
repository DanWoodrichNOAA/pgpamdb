library(DbuddyTools)
library(RPostgres)
library(foreach)
library(tuneR)




#setwd("C:/Apps/pgpamdb")

source("./R/functions.R") #package under construction

source("./R/fw_proj_functions.R") #package under construction

source("./etc/paths.R") #populates connection paths which contain connection variables.

con=pamdbConnect("poc_v2",keyscript,clientkey,clientcert)


#this script will
#inventory data ready to be loaded in ANALYSIS
#using SF metadata in db, calculate 'protocol negatives' between protocol positives
#using 'markingstab' data structure, set max probably detection of hr '2' per vec to human yes/no
#submit detections to db

#inventory data to be loaded in ANALYSIS

#all possible mooring names
possible_moor_names = dbFetch(dbSendQuery(con,"SELECT name,historic_name FROM data_collection"))
lookup=possible_moor_names
possible_moor_names = c(possible_moor_names$name,possible_moor_names$historic_name)
possible_moor_names=possible_moor_names[!is.na(possible_moor_names)]
possible_moor_names=possible_moor_names[!duplicated(possible_moor_names)]



#Search ANALYSIS for these names:

moor_on_ANALYSIS = dir("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS")

moor_on_ANALYSIS=moor_on_ANALYSIS[which(moor_on_ANALYSIS %in% possible_moor_names)]

#great. Now of moor_on_ANALYSIS, check which has correct folder and markings tab

has_folder = dir.exists(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",moor_on_ANALYSIS,
                              "/Detector/TPRthresh_0.99/FD",sep=""))

has_file1 = file.exists(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",moor_on_ANALYSIS,
                              "/Detector/TPRthresh_0.99/FD/",moor_on_ANALYSIS,"_MarkingsTab.txt",sep=""))

newnames = lookup$name[which(lookup$name %in% moor_on_ANALYSIS)]
correspond_oldnames = lookup$historic_name[which(lookup$name %in% newnames)]

newnames = newnames[which(!is.na(correspond_oldnames))]
correspond_oldnames = correspond_oldnames[which(!is.na(correspond_oldnames))]

has_file2 = file.exists(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",newnames,
                              "/Detector/TPRthresh_0.99/FD/",correspond_oldnames,"_MarkingsTab.txt",sep=""))

#first, going to upload data which has marking tab- which seems like maybe all the data?
#the only quirk, is that some of the data has old name- handle this by determining if file exists,
#and if not, load the other one.

#currently loaded moorings:

moorings_done = dbFetch(dbSendQuery(con,"SELECT DISTINCT data_collection.name from soundfiles JOIN data_collection ON
                                     data_collection.id = soundfiles.data_collection_id"))



output = format_FW(con,"BS17_AU_02a")
test = FN_validate(con,output,"//akc0ss-n086/NMML_CAEP_Acoustics/Detector/DetectorRunOutput/FinReview_20200817090230")

output = format_FW(con,"IP18_AU_CH01")
test = FN_validate(con,output,"//akc0ss-n086/NMML_CAEP_Acoustics/Detector/DetectorRunOutput/FinReview_20220217091410")

#try loading this one up
start = Sys.time()
dbAppendTable(con,'detections',output)
end = Sys.time()
#3.685126 minutes for this

#alright- look into the results, on the database.

#the procedure makes it so there should be no 99s in bin_table_wide. Check that (and, make into function!)

query = paste("SELECT COUNT(*) FROM data_collection JOIN soundfiles ON data_collection.id = soundfiles.data_collection_id
JOIN bins ON soundfiles.id = bins.soundfiles_id JOIN bin_label_wide on bins.id = bin_label_wide.id WHERE bins.type = 1
AND data_collection.name = '","IP18_AU_CH01","' AND bin_label_wide.fw != 99",sep="")

query <- gsub("[\r\n]", " ",query)
rslt =dbFetch(dbSendQuery(con,query))

#check everything gets properly deleted (only the IP18_AU_CH01 mooring uploaded):
start = Sys.time()
dbFetch(dbSendQuery(con,"DELETE FROM detections"))
dbFetch(dbSendQuery(con,"DELETE FROM detections"))
end = Sys.time()
#

output = format_FW(con,"BS19_AU_PM08")
test = FN_validate(con,output,"//akc0ss-n086/NMML_CAEP_Acoustics/Detector/DetectorRunOutput/FinReview_20210421092358")




output = format_FW(con,"AL19_AU_IC01")
test = FN_validate(con,output,"//akc0ss-n086/NMML_CAEP_Acoustics/Detector/DetectorRunOutput/FinReview_20210421094053")

output = format_FW(con,"IP19_AU_CH01")
test = FN_validate(con,output,"//akc0ss-n086/NMML_CAEP_Acoustics/Detector/DetectorRunOutput/FinReview_20210421111402")

#this one seems like it was done in error: file 2 missing responses on BB. Fix!
output = format_FW(con,"AL19_AU_BS10")
test = FN_validate(con,output,"//akc0ss-n086/NMML_CAEP_Acoustics/Detector/DetectorRunOutput/FinReview_20210810132417")

output = format_FW(con,"BS19_AU_PM02-b")
test = FN_validate(con,output,"//akc0ss-n086/NMML_CAEP_Acoustics/Detector/DetectorRunOutput/FinReview_20210810171333")

output = format_FW(con,"AL20_AU_PM02-b")
test = FN_validate(con,output,"//akc0ss-n086/NMML_CAEP_Acoustics/Detector/DetectorRunOutput/FinReview_20210811135039")

#looks pretty good! Try out loading one up.

