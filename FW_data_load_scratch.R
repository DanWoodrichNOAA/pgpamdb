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

moor_possible=moor_on_ANALYSIS[which(moor_on_ANALYSIS %in% possible_moor_names)]

#great. Now of moor_on_ANALYSIS, check which has correct folder and markings tab

has_folder = dir.exists(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",moor_possible,
                              "/Detector/TPRthresh_0.99/FD",sep=""))

has_file1 = file.exists(paste("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/",moor_on_ANALYSIS,
                              "/Detector/TPRthresh_0.99/FD/",moor_on_ANALYSIS,"_MarkingsTab.txt",sep=""))


moor_possible = moor_possible[has_folder]

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
fw_done = dbFetch(dbSendQuery(con,"SELECT DISTINCT data_collection.name,data_collection.historic_name from soundfiles JOIN data_collection ON
                                     data_collection.id = soundfiles.data_collection_id JOIN detections ON detections.start_file = soundfiles.id"))

#this isn't considering old vs new names... oops! Means I have been submitting a lot of redundant moorings.
#fixed
moor_to_go = moor_possible[-which(moor_possible %in% c(fw_done$name,fw_done$historic_name))]

#moor_to_go_skip2 = moor_to_go[3:length(moor_to_go)]

time_taken = list()

#do a mega loop:

for(i in 1:length(moor_to_go)){
  try({
  print("Data retrieval and formatting time:")
  start = Sys.time()
  output = format_FW(con,moor_to_go[i])
  endtime = difftime(Sys.time(),start,units="secs")
  time_taken[[i]] = endtime
  print(endtime)
  start = Sys.time()
  dbAppendTable(con,'detections',output)
  endtime = difftime(Sys.time(),start,units="secs")
  print("DB upload time:")
  print(endtime)
  time_taken[[i]] = time_taken[[i]] + endtime
  })

}

#problem moorings:
#1 "AL19_AU_PH01": error in `$<-.data.frame`(`*tmp*`, "CallType", value = "FN") : replacement has 1 row, data has 0
#-solution: bugfix where can now consider case where all detections accounted for in FDresult
#2 "AW13_AU_BS1":Error in format_FW(con, moor_to_go[i]) : assumption error- labels conflict between original designation and back-calculated designation
#-bug: looks like BB data is duplicated as both yes and no. Probably a bug in initial comparison- rounding difference (slightly different values)
#solution- rounding wouldn't work confusingly. So just using selection as id, which I think should be a safe assumption.
#warning- keep an eye out for cases where detections are duplicated- may be an indicator of rounding issues not caught. However,
#likely none went through, as this introduced a lot of assumption errors in this one case.

#3 "AW15_AU_BS1":Error in if ((row$label == 21 & MarkTab_reduce$MarkVec[i] == "n") | (row$label ==  : missing value where TRUE/FALSE needed
#this one, the markingstab data is messed up (contains NAs) so probably just needs to be re-reviewed.
#list of all of these: "BS15_AU_05a", "AW15_AU_BS1","AW15_AU_BS3","AL19_AU_BS10"

#"RW09_EA_02" this has the case of having no detections above .90 or .91. So just need an if statement to apply to


#repeated moors:

repeated_moors = dbFetch(dbSendQuery(con,"SELECT data_collection.name,COUNT(DISTINCT modified) from detections JOIN soundfiles on detections.start_file = soundfiles.id JOIN data_collection ON
                                     data_collection.id = soundfiles.data_collection_id GROUP BY data_collection.name"))
#if value is greater than 1, need to delete the extras!

repeated_moors = repeated_moors$name[which(repeated_moors$count>1)]

#just delete all of the repeated moorings, and try again.
del_ids = dbFetch(dbSendQuery(con,paste("SELECT detections.id FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id JOIN data_collection
                              ON data_collection.id = soundfiles.data_collection_id WHERE data_collection.name IN ('",paste(repeated_moors,collapse="','",sep=""),"')",sep="")))

start = Sys.time()
table_delete(con,'detections',as.integer(del_ids$id),hard_delete = TRUE)
endtime = difftime(Sys.time(),start,units="secs")


output = format_FW(con,"BS17_AU_02a")
test = FN_validate(con,output,"//akc0ss-n086/NMML_CAEP_Acoustics/Detector/DetectorRunOutput/FinReview_20200817090230")

output = format_FW(con,"IP18_AU_CH01")
test = FN_validate(con,output,"//akc0ss-n086/NMML_CAEP_Acoustics/Detector/DetectorRunOutput/FinReview_20220217091410")

#try loading this one up
start = Sys.time()
dbAppendTable(con,'detections',output)
endtime = difftime(Sys.time(),starttime,units="secs")
print(endtime)
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

initial_moorings = c("BS17_AU_02a","BS19_AU_PM08","AL19_AU_IC01","IP19_AU_CH01","AL19_AU_BS10","BS19_AU_PM02-b","AL20_AU_PM02-b")

for(i in 1:length(initial_moorings)){
  print("Data retrieval and formatting time:")
  start = Sys.time()
  output = format_FW(con,"BS19_AU_PM08")
  endtime = difftime(Sys.time(),start,units="secs")
  print(endtime)
  start = Sys.time()
  dbAppendTable(con,'detections',output)
  endtime = difftime(Sys.time(),start,units="secs")
  print("DB upload time:")
  print(endtime)
}
#try loading this one up


#oops! alright, so made a mistake and loaded BS19_AU_PM08 7 times instead of each individual one. Try out deletion!
#Do it in a time based fashion- find the ids of the detections that occured after the original upload, and do a hard delete of these.

#in the future, this kind of query could be catastophic unless I also time bounded it with a less than, and made sure the contents
#were only what I uploaded.
del_ids = dbFetch(dbSendQuery(con,"SELECT id FROM detections WHERE modified > '2022-12-15 01:35:00'"))

start = Sys.time()
table_delete(con,'detections',as.integer(del_ids$id),hard_delete = TRUE)
endtime = difftime(Sys.time(),start,units="secs")

#time to delete 6 fin moorings (BS19_AU_PM08 x 6) :

#try this again:

for(i in 1:length(initial_moorings)){
  print("Data retrieval and formatting time:")
  start = Sys.time()
  output = format_FW(con,initial_moorings[i])
  endtime = difftime(Sys.time(),start,units="secs")
  print(endtime)
  start = Sys.time()
  dbAppendTable(con,'detections',output)
  endtime = difftime(Sys.time(),start,units="secs")
  print("DB upload time:")
  print(endtime)
}






