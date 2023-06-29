#hb_inv

#cut down on keystrokes
dbGet <-function(x){
  x = gsub("[\r\n]", "", x)
  dbFetch(dbSendQuery(con,x))
}

library(RPostgres)
library(ggplot2)

#setwd("C:/Apps/pgpamdb")

source("./R/functions.R") #package under construction

source("./etc/paths.R") #populates connection paths which contain connection variables.

#as I use this script, I'm just deleting and changing code to create the lookup table. to upload fgs only, I only use the first
#part of the loop below

con=pamdbConnect("poc_v2",keyscript,clientkey,clientcert)

#identify datasets which have labeled hb
##MW dataset: assumption is randomly (from SC hb: exactly how again?) sampled, and fully annotated for specific song call types.
##jenna og dataset: identified hb n, ns, s, in a smaller sample.

#at end for each, want:
#procedure/assumptions
#stats on sampling extent
#start on # of boxed calls.

procedures = dbGet("SELECT * FROM procedures")
signals = dbGet("SELECT * FROM signals")

#11 sampling extent.(from fg)

#pull the fg, which are...
#XP14_UK_KO01_sample1
#XP15_UK_KO01_sample1
#round1_pull1_reduce (afsc mw/jenna effort)
#round1_pull2 (afsc mw effort)

allfgs = dbGet("SELECT bins.seg_end-bins.seg_start AS bindur,data_collection.location_code,soundfiles.datetime,effort.name,bins.type FROM bins JOIN bins_effort ON bins.id =
               bins_effort.bins_id JOIN effort ON bins_effort.effort_id = effort.id JOIN soundfiles ON bins.soundfiles_id= soundfiles.id JOIN data_collection ON data_collection.id =
               soundfiles.data_collection_id WHERE effort.name IN ('XP14_UK_KO01_sample1','XP15_UK_KO01_sample1',
               'round1_pull1_reduce','round1_pull2')")

allfgs$month = format(allfgs$datetime,"%m")

effort_agg = aggregate(allfgs$bindur,list(as.numeric(allfgs$month),as.factor(allfgs$location_code)),FUN= sum)

effort_agg$hours = round(effort_agg$x/3600,2)
effort_agg$x = NULL
colnames(effort_agg) = c("month","site",'count')

eff_table  = reshape(effort_agg, idvar = "site", timevar = "month", direction = "wide")
eff_table = eff_table[,c(1,6,2,3,4,5,7)]

# stats on boxed calls
dataset = dbGet("SELECT detections.*,soundfiles.datetime,data_collection.location_code FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id JOIN data_collection ON
                data_collection.id = soundfiles.data_collection_id WHERE detections.procedure = 11")

dataset_pos = dataset[which(dataset$label==1 & dataset$status==1),]
dataset_pos$month = format(dataset_pos$datetime,"%m")

#missing location_code for some of these, indicating we might not have hawaii data in data_collection?
test = aggregate(dataset_pos,list(as.numeric(dataset_pos$month),as.factor(dataset_pos$location_code)),length)
test = test[,1:3]
colnames(test) = c("month","site",'count')

counts_table  = reshape(test, idvar = "site", timevar = "month", direction = "wide")
counts_table = counts_table[,c(1,6,2,3,4,5,7)]



#not very descriptive w/o effort... instead, let's just take total counts from hawaii vs afsc.
hawaii_counts = sum(dataset_pos$location_code=="KN01")
afsc_counts = nrow(dataset_pos) - sum(dataset_pos$location_code=="KN01")


#MW dataset
#procedure 11
#samples were pulled from the 2015-2016 year in both kona and afsc. The kona sampling differed from the afsc sampling
#. the afsc sampling determined consecutive hb (waves?), and pulled a number of consecutive sections randomly. kona
#sampling was random 1hr segments.
#extent:
#calls: 15324 total labeled calls, 5535 from afsc


#og jenna dataset:

allfgs2 = dbGet("SELECT bins.seg_end-bins.seg_start AS bindur,data_collection.location_code,soundfiles.datetime,effort.name,bins.type FROM bins JOIN bins_effort ON bins.id =
               bins_effort.bins_id JOIN effort ON bins_effort.effort_id = effort.id JOIN soundfiles ON bins.soundfiles_id= soundfiles.id JOIN data_collection ON data_collection.id =
               soundfiles.data_collection_id WHERE effort.name IN ('HB_s_ns_sample1')")

allfgs2$month = format(allfgs2$datetime,"%m")

effort_agg2 = aggregate(allfgs2$bindur,list(as.numeric(allfgs2$month),as.factor(allfgs2$location_code)),FUN= sum)

effort_agg2$hours = round(effort_agg2$x/3600,2)
effort_agg2$x = NULL
colnames(effort_agg2) = c("month","site",'count')

eff_table2  = reshape(effort_agg2, idvar = "site", timevar = "month", direction = "wide")
eff_table2 = eff_table2[,c(1,6,2,3,4,5,7)]

#dets from this
dataset2 = dbGet("SELECT detections.*,soundfiles.datetime,data_collection.location_code FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id JOIN data_collection ON
                data_collection.id = soundfiles.data_collection_id WHERE detections.procedure = 14")

total_dur = nrow(dataset2)*10
total_dur = total_dur/3600

