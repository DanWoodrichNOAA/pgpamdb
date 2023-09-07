#in this script, try to write a few plotting functions to help visualize db results in a zoomed out way.
#heatmap by month and latitude divisions might be a good one!

#so to start it off, plotting by 'yes' bins vs 'no' bins + 'uk' bins throughout the data!

#query pulls sum of the above (in type 1)

#cut down on keystrokes
dbGet <-function(x){
  x = gsub("[\r\n]", "", x)
  dbFetch(dbSendQuery(con,x))
}

library(RPostgres)
library(ggplot2)
library(dplyr)

#setwd("C:/Apps/pgpamdb")

source("./R/functions.R") #package under construction

source("./etc/paths.R") #populates connection paths which contain connection variables.

#as I use this script, I'm just deleting and changing code to create the lookup table. to upload fgs only, I only use the first
#part of the loop below

con=pamdbConnect("poc_v2",keyscript,clientkey,clientcert)


fw_map = bin_label_explore(con,'fw')

fw_map_wgt =fw_map + add_layer_ble(con,6,10,"orange")
fw_map_wgt = fw_map_wgt + add_layer_ble(con,5,10,"red")

plot(fw_map_wgt)

#little odd- not a very good distribution vs what is there! recall, only 7 comparison moorings were even available.


#couple areas to common sense spot check- looks like there was training from a month where no fw was detected- check it out
#also, looks like there was a y in CH01- an accident? make a query and go see it.

#training is correct- no postives in training.
#High latitude positives- there is a mixed bag here. some obviously incorrect (WT01), some possibly incorrect (likely ag),
#some definitely correct. After I have tools to edit labels of deployment detections, correct obvious errors and send
#the rest around.

#to check out the existing data sources I want to make a non-overlapping fg composed of all training from fn and bb.

fn_1genFNfgs = dbGet("SELECT id,name FROM effort WHERE name LIKE '%fn%'")
fn_1genBBfgs = dbGet("SELECT id,name FROM effort WHERE name LIKE '%bb%'")

all_eff_ids = as.integer(c(fn_1genFNfgs$id,fn_1genBBfgs$id))

fw_og_train = dbGet(paste("SELECT bins.id FROM bins JOIN bins_effort ON bins_effort.bins_id = bins.id JOIN
                        effort ON effort.id = bins_effort.effort_id WHERE bins.type = 1 AND effort.id IN (",paste(all_eff_ids,sep="",collapse=","),")",sep=""))
fw_og_train = as.integer(fw_og_train$id)

fw_og_train_nodup = fw_og_train[-which(duplicated(fw_og_train))]

#alright, this is our combined fg. resubmit.

effort_table = data.frame("fw_og_train",
                          "high grade semi-random no overlap",
                          "The ground truth training used in the previous FN/BB detectors (keyword fn_hg and bb_hg), with internal overlap removed."
)
colnames(effort_table) = c('name','sampling_method','description')

#dbAppendTable(con,'effort',effort_table)

#get new id

db_eff_row = dbGet("SELECT * FROM effort WHERE name = 'fw_og_train'")

effort_procedures = data.frame(as.integer(db_eff_row$id),10,4,"train_eval","i_neg","n")
colnames(effort_procedures) = c("effort_id","procedures_id","signal_code","effproc_type","effproc_assumption","completed")

#dbAppendTable(con,'effort_procedures',effort_procedures)

allbins = data.frame(fw_og_train_nodup,as.integer(db_eff_row$id))

colnames(allbins) = c("bins_id","effort_id")

#dbAppendTable(con,'bins_effort',allbins)

#alright, now I can take a peek at the fg and decide if subsampling makes sense.

#first observation- data looks good, except for the bb from AW12_AU_CL01_files_441-516_bb_hg, which is bugged.
#so, need to reupload these.
#steps:
#1. delete bb dets on procedure 10 from fg
#2. retrieve original data, reformat and reupload.
#3. check data.

#double checking it is indeed bugged. fg looks correct, but doesn't look correctly displayed in raven.
#looks like the fg is wrong, and then detections are wrong. So- will need to remake the previous fg in addition after correcting
#this one.

#every row starting and after AU-AWCL01-130725-114000.wav is not in original
#every row prior to AU-AWCL01_20120926_145000.wav (300s start) is not in original.

#so, need to remove extra trailing data, and discard and recalc bb dets and i_neg
#and, can actually just remove these same detections from the new fg I made without redoing it.

#so, should i indeed discard the bb detections from the fg? Yeah, probably. do that first

del_ids = dbGet("SELECT DISTINCT(detections.*) FROM detections JOIN bins_detections ON detections.id = bins_detections.detections_id JOIN bins ON
                 bins_detections.bins_id = bins.id JOIN bins_effort ON bins_effort.bins_id = bins.id JOIN effort ON bins_effort.effort_id = effort.id
                 WHERE procedure = 10 AND signal_code = 6 AND effort.name =  'AW12_AU_CL01_files_441-516_bb_hg'")

#table_delete(con,'detections',as.integer(del_ids$id),hard_delete = TRUE)

#then- edit existing fgs to be correct.
#pull all bins with datetime, and then add segstart to datetime.

bins_to_rem = dbGet("SELECT bins.*,soundfiles.datetime FROM bins JOIN bins_effort ON bins_effort.bins_id = bins.id JOIN effort ON bins_effort.effort_id = effort.id
                     JOIN soundfiles ON soundfiles.id = bins.soundfiles_id WHERE effort.name =  'AW12_AU_CL01_files_441-516_bb_hg'")

bins_to_rem$datetime_wseg = bins_to_rem$datetime + bins_to_rem$seg_start

bins_to_rem_del = bins_to_rem[which(bins_to_rem$datetime_wseg<as.POSIXct("2012-09-26 14:55:00",tz="utc")|
                                      bins_to_rem$datetime_wseg>as.POSIXct("2013-07-25 11:35:00",tz="utc")),]

#so, these are bins to delete from bb training fg and from the combined fg.
bins_to_rem_del_ids = as.integer(bins_to_rem_del$id)

test_del = dbGet(paste("SELECT * FROM bins_effort JOIN effort ON bins_effort.effort_id= effort.id WHERE effort.name IN ('AW12_AU_CL01_files_441-516_bb_hg',
                 'fw_og_train') AND bins_effort.bins_id IN (",paste(bins_to_rem_del_ids,sep="",collapse=","),")",sep=""))

del_ids = unique(test_del$bins_id)

#oops, accidentally deleted these from one more fg than I meant to... probably the fn one.
#table_delete(con,'bins_effort',as.integer(del_ids),idname = 'bins_id')

#table = data.frame(as.integer(del_ids),217)
#colnames(table) = c("bins_id","effort_id")
#add back in
#dbAppendTable(con,'bins_effort',table)

#all fixed now.

#now, ready to load back in the bb detections.

fg= dbFetch(dbSendQuery(con,"SELECT bins.*,soundfiles.name,soundfiles.duration,soundfiles.datetime FROM bins JOIN bins_effort ON bins_effort.bins_id = bins.id JOIN effort ON bins_effort.effort_id = effort.id
                     JOIN soundfiles ON soundfiles.id = bins.soundfiles_id WHERE effort.name =  'AW12_AU_CL01_files_441-516_bb_hg'"))

#rav_og_data = read.delim(bb_path)

#put fg in cons order

fg = fg[order(fg$name,fg$seg_start),]

#fg$cons = c(0,cumsum(fg$seg_end-fg$seg_start)[1:length(cumsum(fg$seg_end-fg$seg_start))-1])

dets=read.delim("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/RavenBLEDscripts/Data/Selection tables/BB/AW12_AU_CL1Sum/AW12_AU_CL1_files_441-516.txt")

dets$dur = dets$End.Time..s.-dets$Begin.Time..s.

#make new dataframe, based on size of files and not bins.
maxs = aggregate(fg$seg_end,list(fg$soundfiles_id),max)
mins = aggregate(fg$seg_start,list(fg$soundfiles_id),min)

fgtemp = fg[-which(duplicated(fg$soundfiles_id)),]

justfiles = data.frame(maxs$Group.1,mins$x,maxs$x,fgtemp[match(maxs$Group.1,fgtemp$soundfiles_id),c("type","name","duration")])
justfiles=justfiles[order(justfiles$name),]
colnames(justfiles) = colnames(fg)[2:(length(colnames(fg))-1)]

justfiles$cons = c(0,cumsum(justfiles$seg_end-justfiles$seg_start)[1:length(cumsum(justfiles$seg_end-justfiles$seg_start))-1])

#add an additional 300s on to dets, to account for initial offset.

dets$start_file = justfiles$soundfiles_id[findInterval(dets$Begin.Time..s.,justfiles$cons)]
dets$end_file = justfiles$soundfiles_id[findInterval(dets$End.Time..s.,justfiles$cons)]

dets$Begin.Time..s. = dets$Begin.Time..s. - justfiles$cons[findInterval(dets$Begin.Time..s.,justfiles$cons)]
dets$End.Time..s. = dets$End.Time..s. - justfiles$cons[findInterval(dets$End.Time..s.,justfiles$cons)]

#add back the 300s for the files which started on 300s.
justfilestemp = justfiles[,c("soundfiles_id","seg_start")]
colnames(justfilestemp)[1]="start_file"
dets = merge(dets,justfilestemp)

dets$Begin.Time..s. = dets$Begin.Time..s.+ dets$seg_start
dets$End.Time..s. = dets$End.Time..s.+ dets$seg_start

template = dbFetch(dbSendQuery(con,'SELECT * FROM detections LIMIT 1'))

input = data.frame(dets$Begin.Time..s.,dets$End.Time..s.,dets$Low.Freq..Hz.,dets$High.Freq..Hz.,
                   dets$start_file,dets$end_file,NA,"",10,1,6,2)

colnames(input)=colnames(template)[2:(length(input)+1)]

#add i_neg

i_neg_out = i_neg_interpolate(input,fg,64,procedure = 10,signal_code = 6,analyst = 2)

input$analyst= 2

input_all = rbind(input,i_neg_out)

#dbAppendTable(con,'detections',input_all)

#looks fixed! But, likely needs a review pass based on the quality of the annotations.
#so, back to original aim- assess all fg and decide to subsample or not.


#another inquiry: query all of  the fin comments. Read through them to see if there is anything compelling and relevant to training.

#all_comments = dbGet("SELECT detections.*,soundfiles.name FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id WHERE
#                      procedure IN (6,7,8,9) AND signal_code IN (4,5,6) AND comments != '' LIMIT 50")

#doesn't look I made many comments directly on the analysis. from a quick look doesn't look untrue, but hard to say.
#either way, that's a dead end.

#I am thinking it might not make too much sense to undersample first- I can create the full set, and make a subset of it
#later if that improves training .

#steps:
#1.load in all dets from fg
#2.remove bb detections which have same start and end time and start and end file as fns.
#3. recategorize each as fw detections
#4. recalc bin negs
#5. submit to db.

#then what?
#I perform a stratified sample on the data. Do it by # of detections (up to a cap), to include more data from light sections
#than heavy sections.
#so the logic will be something like: pull detections/bins, group by month_y_site, limit by 250 detections, and then use
#the remaining bins .

#I can just pull the bin ids, and then declare like 4 different random subsamples of these. anaylse 1,2, or 3 of them.

#actually, I think the easier way to do this is just pull from bins. This will naturally favor the more sparse detections,

strat_sample_bins = dbGet("SELECT DISTINCT on (bins.id) detections.*,bins.*,soundfiles.name FROM detections JOIN bins_detections
                           ON bins_detections.detections_id = detections.id JOIN bins ON bins_detections.bins_id = bins.id JOIN
                           soundfiles ON soundfiles.id = bins.soundfiles_id WHERE label IN (1,21) AND procedure = 6  ")

dbGet(
"SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY date_trunc('month', datetime)) AS r,
        t.* FROM soundfiles t ) x WHERE x.r <= 3 LIMIT 50"
)

#building it slowly. This is the subquery, but, want to have a over partition additional sample on it.
#for this sample, I want somewhat sparse but not megasparse, so I will mandate 3 dets per bin.
#I will do a seperate sample for negative data.
test =dbGet("SELECT * FROM (SELECT bins.id,date_trunc('month', soundfiles.datetime) AS dt_,data_collection.id,COUNT(*) as dets_in
               FROM detections JOIN bins_detections ON bins_detections.detections_id = detections.id JOIN bins ON bins_detections.bins_id = bins.id JOIN
               soundfiles ON soundfiles.id = bins.soundfiles_id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
              WHERE label IN (1,21) AND procedure = 6 AND bins.type = 1 GROUP BY bins.id,dt_,data_collection.id) AS subquery WHERE subquery.dets_in >= 3")

test$comb = paste(test$dt_,as.numeric(as.factor(test$id..3)))
library(dplyr)

strat_sample <- test %>%
  group_by(comb) %>%
  sample_n(size=5,replace=TRUE)

strat_sample = strat_sample[-which(duplicated(strat_sample)),]

strat_sample$dets_in = as.integer(strat_sample$dets_in)

#further reduce: for each category, limit to one "high detection" row.

strat_sample = strat_sample[sample(1:nrow(strat_sample)),]
strat_sample$ishigh=as.numeric(strat_sample$dets_in>50)
strat_sample$ismed=as.numeric(strat_sample$dets_in>15)
strat_sample$cathigh = paste(strat_sample$comb,strat_sample$ishigh)
strat_sample = strat_sample[which(strat_sample$ishigh==0 | !duplicated(strat_sample$cathigh)),]

hist(as.numeric(table(strat_sample$comb)))

#of the strat sample, split into samples which contain high, medium, and low. seperate into different fgs so I can
#control how much data included, and perf eval will be more informative splitting these up.

comb_samp = data.frame(strat_sample$comb,as.numeric(as.factor(strat_sample$comb)))
comb_samp$rand_key = rep(1:5,length.out =nrow(comb_samp))

strat_sample$comb_samp = comb_samp$rand_key[match(as.numeric(as.factor(strat_sample$comb)),comb_samp$as.numeric.as.factor.strat_sample.comb..)]

high_combs = strat_sample$comb[which(strat_sample$ishigh==1)]
med_combs = unique(strat_sample$comb[which(strat_sample$ismed==1)])
med_combs = med_combs[-which(med_combs %in% high_combs)]
low_combs = unique(strat_sample$comb)[-which(unique(strat_sample$comb) %in% c(high_combs,med_combs))]

strat_sample_high = strat_sample[which(strat_sample$comb %in% high_combs),]
strat_sample_med = strat_sample[which(strat_sample$comb %in% med_combs),]
strat_sample_low = strat_sample[which(strat_sample$comb %in% low_combs),]

strat_sample_med$comb_samp = strat_sample_med$comb_samp+5
strat_sample_high$comb_samp = strat_sample_high$comb_samp+10

#now, publish these.

effort_fgs = data.frame(paste("fw2gen_strat_pos_",rep(c("fewfin","somefin","manyfin"),each=5),rep(1:5,length.out=15),sep=""),"high grade random",
                        paste("stratified sample. pulled bins with > 3 fw 1 gen detections out of db (positive sample). Further sampled this by taking a stratified sample of each month/mooring id. Then, for each of those categories, undersample data within each of these categories by removing instances where more than 1 bin per category had 50 detections. Then, broke the month/mooring ids down by whether they had many (>50), some (>15) or few (<15), and split each of those components into 5 smaller chunks (keeping together month/mooring id category)"))

colnames(effort_fgs)= c("name","sampling_method","description")

#dbAppendTable(con,'effort',effort_fgs)

ids_key = dbGet(paste("SELECT id,name FROM effort WHERE name IN ('",paste(effort_fgs$name,sep="",collapse="','"),"')",sep=""))
ids_key$lookup = 1:15

strat_sample_low$comb_samp = ids_key[match(strat_sample_low$comb_samp,ids_key$lookup),"id"]
strat_sample_med$comb_samp = ids_key[match(strat_sample_med$comb_samp,ids_key$lookup),"id"]
strat_sample_high$comb_samp = ids_key[match(strat_sample_high$comb_samp,ids_key$lookup),"id"]

#now, assemble and submit to bins_effort.

input = data.frame(as.integer(c(strat_sample_low$id,strat_sample_med$id,strat_sample_high$id)),as.integer(c(strat_sample_low$comb_samp,strat_sample_med$comb_samp,strat_sample_high$comb_samp)))

colnames(input)=c("bins_id","effort_id")

#dbAppendTable(con,"bins_effort",input)

#last to populate is the effort_procedures

effort_procedures = data.frame(as.integer(ids_key$id),10,4,"train_eval","i_neg","n")

colnames(effort_procedures) = c("effort_id","procedures_id","signal_code","effproc_type","effproc_assumption","completed")

#dbAppendTable(con,'effort_procedures',effort_procedures)

#now, I will break these into further divisions. Break them into 5ths, by comb. however, do this before further splitting for
#simplicity.

#now, break them into their component parts, and publish together.

#this gets me the bins- I have 7911 total = 660 hours. So, I need to review only a portion of this. How much do I have for
#original training?

dbGet("SELECT COUNT(*) FROM bins_effort JOIN effort ON bins_effort.effort_id = effort.id WHERE effort.name = 'fw_og_train'")

#so, the total pool is about 4 times the sie of og training


#nice, now I can get started boxing them. Try to box first one of each fin occurence category, and if I can complete it
#go from there.

#other samples to pull: could get a stratified negatives sample (may not even need to review)
#hard negatives sample (again, could try to not exhaustively review this)
#then, need to add the specifically called out weird fin sections.

#observed: looks like detections are a little offset from what they should be in BS09_AU_PM02-a (seen in AU-BSPM02_a-090624-121721.wav)
#need to fix at some point.

#might try to readjust these instead of going back in to the whole upload shebang- see if I can identify the rate of drift
#and correct linearly.

#0618: 4s off (ahead)
#0714: 1.25s off (ahead)
#0725: 2s off (ahead)
#0813: 3.5s off (ahead)
#0817: dead on
#0909: 1.5s off (ahead)

#notes
#AU-BSPM02_a-090816-221804.wav : resets to correct, where before it was about 3.5s off. starts to creep up from here.

#verdict- no easy way to adjust these to be correct. probably need to reupload. Use the original times like I did
#in the low moan reupload for the same mooring.

#steps:
#download all of the fin detections (bb,fn)
#save them to temporary copy
#delete them (and bin negatives)
#load in original dets, and original sfiles doc, recalc the dets.

#it actually look like the original exported detections (out of mastor_detecter) are consistent with off times.
#so, can't really backtrack further.

#other (lazy) option- just forget about these, delete and remove from samples. Wait to rerun it once new detector is available.

#also observed: some overlap with initial training, so need to scrub these overlapping bins from the samples.


#I think what I do is I soft 1. delete these- work it out later if desired.
#2. do not change the original sample, since it still should apply the same.
#3. do remove duplicated bins from og sample in stratified sample.
#4. resubmit all detections from sample as signal code 4 procedure 10 detections, and then query only those in review
#5. begin boxing.



BS09_AU_PM02_a_dels2 = dbGet("SELECT * FROM detections JOIN soundfiles ON detections.start_file = soundfiles.id WHERE soundfiles.data_collection_id = 144 AND
                  detections.signal_code IN (4,5,6) AND detections.procedure IN (6,7,8)")

#len before delete: 655045
#len after delete: 655045

#table_delete(con,'detections',as.integer(BS09_AU_PM02_a_del$id),hard_delete = FALSE)

allbins_newsamples = dbGet(paste("SELECT bins_id FROM bins_effort WHERE effort_id IN (",paste(ids_key$id,collapse=",",sep=""),")",sep=""))
allbins_ogsample = dbGet("SELECT bins_id FROM bins_effort WHERE effort_id = 253")

allbins_newsamples = as.integer(allbins_newsamples$bins_id)
allbins_ogsample = as.integer((allbins_ogsample$bins_id))

#yeah, cleanest I think just to remove dups from the original sample.
dups = allbins_newsamples[which(allbins_newsamples %in% allbins_ogsample)]

#dbGet(paste("DELETE FROM bins_effort WHERE bins_id IN (",paste(dups,collapse=",",sep=""),") AND effort_id IN (",paste(ids_key$id,collapse=",",sep=""),")",sep=""))

#deleted 24 rows, no problem.

#resubmit detections as 10s- do this just for the initial one at the start: fw2gen_strat_pos_fewfin1

dets_in_1st = dbGet("SELECT detections.* FROM detections JOIN bins_detections ON detections.id = bins_detections.detections_id JOIN bins ON
                     bins_detections.bins_id = bins.id JOIN bins_effort ON bins_effort.bins_id = bins.id JOIN effort ON effort.id =
                     bins_effort.effort_id WHERE status = 1 AND signal_code IN (5,6) AND label IN (1,21) AND procedure = 6 AND effort.id = 269")

dets_in_1st$modified = NULL
dets_in_1st$id= NULL
dets_in_1st$probability = NA
dets_in_1st$label = 1
dets_in_1st$signal_code = 4
dets_in_1st$procedure = 10

#dbAppendTable(con,'detections',dets_in_1st)

#looking good, now can start boxing.

#maybe bugged? BS16_AU_PM05\07_2017\AU-BSPM05-170705-050000.wav
#maybe bugged: RW11_EA_BS03\11_2011\EA-RWBS03-111105-015743.wav
#maybe bugged: RW11_EA_BS03\02_2012\EA-RWBS03-120207-065743.wav
#maybe bugged: AL19_AU_UM01\10_2020\AU-ALUM01-201015-060000.wav
#maybe bugged: BS16_AU_PM05\07_2017\AU-BSPM05-170712-075000.wav
#offset problem (boxes 1s after signal): RW10_EA_BS02\09_2010\EA-RWBS02-100927-080816.wav
#offset problem (boxes 1s after signal): RW10_EA_BS02\09_2010\EA-RWBS02-100929-200817.wav


#do this again for next fg. But this time, just remove either fn or bb (whichever has fewer dets)

dets_in_2nd = dbGet("SELECT detections.* FROM detections JOIN bins_detections ON detections.id = bins_detections.detections_id JOIN bins ON
                     bins_detections.bins_id = bins.id JOIN bins_effort ON bins_effort.bins_id = bins.id JOIN effort ON effort.id =
                     bins_effort.effort_id WHERE status = 1 AND signal_code IN (5,6) AND label IN (1,21) AND procedure = 6 AND effort.id = 274")

dets_in_2nd$modified = NULL
dets_in_2nd$id= NULL
dets_in_2nd$probability = NA
dets_in_2nd$label = 1

fn_ = dets_in_2nd[which(dets_in_2nd$signal_code==5),]
bb_ = dets_in_2nd[which(dets_in_2nd$signal_code==6),]

bb_$temp_id= 1:nrow(bb_)

for(i in 1:nrow(fn_)){

  fn_row = fn_[i,]

  bb_comp = bb_[which(bb_$start_file==fn_row$start_file),]
  if(nrow(bb_comp)>0){
    for(p in 1:nrow(bb_comp)){
      bb_row = bb_comp[p,]


      if((bb_row$start_time>=fn_row$start_time & bb_row$end_time<=fn_row$end_time) |
         (bb_row$start_time<=fn_row$start_time & bb_row$end_time>=fn_row$end_time) |
         (bb_row$end_time>= fn_row$start_time & bb_row$end_time<=fn_row$end_time) |
         (bb_row$start_time>= fn_row$start_time & bb_row$start_time<=fn_row$end_time)){

        #print(p)
        #print(rbind(fn_row,bb_row))

        fn_[i,"start_time"] = min(fn_row$start_time,bb_row$start_time)
        fn_[i,"end_time"] = max(fn_row$end_time,bb_row$end_time)
        fn_[i,"high_freq"] = max(fn_row$high_freq,bb_row$high_freq)
        fn_[i,"low_freq"] = max(fn_row$low_freq,bb_row$low_freq)

        bb_ = bb_[-which(bb_$temp_id==bb_row$temp_id),]

        #this will make it so we don't overcombine lots of overlapping into single detection.
        break
      }
    }
  }

}


bb_$temp_id = NULL

dets_in_2nd_ = rbind(fn_,bb_)

dets_in_2nd_$signal_code = 4
dets_in_2nd_$procedure = 10

#submit
#dbAppendTable(con,'detections',dets_in_2nd_)


#note - m5 has some sections where it looks like mooring noise, but patterning indicates it is indeed fin: ex:
#BS15_AU_PM05\10_2015\AU-BSPM05-151014-133000.wav

dets_in_3rd = dbGet("SELECT detections.* FROM detections JOIN bins_detections ON detections.id = bins_detections.detections_id JOIN bins ON
                     bins_detections.bins_id = bins.id JOIN bins_effort ON bins_effort.bins_id = bins.id JOIN effort ON effort.id =
                     bins_effort.effort_id WHERE status = 1 AND signal_code IN (5,6) AND label IN (1,21) AND procedure = 6 AND effort.id = 279")

dets_in_3rd$modified = NULL
dets_in_3rd$id= NULL
dets_in_3rd$probability = NA
dets_in_3rd$label = 1

fn_ = dets_in_3rd[which(dets_in_3rd$signal_code==5),]
bb_ = dets_in_3rd[which(dets_in_3rd$signal_code==6),]

bb_$temp_id= 1:nrow(bb_)

for(i in 1:nrow(fn_)){

  fn_row = fn_[i,]

  bb_comp = bb_[which(bb_$start_file==fn_row$start_file),]
  if(nrow(bb_comp)>0){
    for(p in 1:nrow(bb_comp)){
      bb_row = bb_comp[p,]


      if((bb_row$start_time>=fn_row$start_time & bb_row$end_time<=fn_row$end_time) |
         (bb_row$start_time<=fn_row$start_time & bb_row$end_time>=fn_row$end_time) |
         (bb_row$end_time>= fn_row$start_time & bb_row$end_time<=fn_row$end_time) |
         (bb_row$start_time>= fn_row$start_time & bb_row$start_time<=fn_row$end_time)){

        #print(p)
        #print(rbind(fn_row,bb_row))

        fn_[i,"start_time"] = min(fn_row$start_time,bb_row$start_time)
        fn_[i,"end_time"] = max(fn_row$end_time,bb_row$end_time)
        fn_[i,"high_freq"] = max(fn_row$high_freq,bb_row$high_freq)
        fn_[i,"low_freq"] = max(fn_row$low_freq,bb_row$low_freq)

        bb_ = bb_[-which(bb_$temp_id==bb_row$temp_id),]

        #this will make it so we don't overcombine lots of overlapping into single detection.
        break
      }
    }
  }

}


bb_$temp_id = NULL

dets_in_3rd_ = rbind(fn_,bb_)

dets_in_3rd_$signal_code = 4
dets_in_3rd_$procedure = 10

#submit
#dbAppendTable(con,'detections',dets_in_3rd_)


#visualize the new samples:

#results of all lm data and deployment.
fw_map = bin_label_explore(con,'fw',plot_sds = 2)
plot(fw_map)

#create the layers:
fw1 = add_layer_ble(con,5,10,'orange',c("fw_og_train"))
fw2 = add_layer_ble(con,6,10,'orange',c("fw_og_train"))

ff1 = add_layer_ble(con,4,10,'blue',c("fw2gen_strat_pos_fewfin1"))
sf1 = add_layer_ble(con,4,10,'blue',c("fw2gen_strat_pos_somefin1"))
mf2 = add_layer_ble(con,4,10,'blue',c("fw2gen_strat_pos_manyfin1"))

#original
plot(fw_map + fw1 + fw2)

#all new
plot(fw_map + ff1 + sf1 + mf2)

#all:
plot(fw_map + ff1 + sf1 + mf2 + fw1 + fw2)

#the next FG I want to introduce is hard negatives. Make it similarly sized to others

bins_fw_og_train_size= dbGet("SELECT COUNT(*) FROM bins_effort JOIN effort ON effort.id = bins_effort.effort_id
                         WHERE effort.name = 'fw_og_train'")

bins_strat_sample1= dbGet("SELECT COUNT(*) FROM bins_effort JOIN effort ON effort.id = bins_effort.effort_id
                         WHERE effort.name IN ('fw2gen_strat_pos_fewfin1','fw2gen_strat_pos_somefin1','fw2gen_strat_pos_manyfin1')")

#total dets in strat sample 1
strat_sample1_total_dets = dbGet("SELECT COUNT(DISTINCT detections.id) FROM detections JOIN bins_detections ON detections.id = bins_detections.detections_id JOIN
                                   bins ON bins_detections.bins_id = bins.id JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON effort.id = bins_effort.effort_id
                                   WHERE detections.signal_code = 4 AND detections.label = 1 AND detections.procedure = 10 AND
                                   effort.name IN ('fw2gen_strat_pos_fewfin1','fw2gen_strat_pos_somefin1','fw2gen_strat_pos_manyfin1')")

fw_og_train_total_dets= dbGet("SELECT COUNT(DISTINCT detections.id) FROM detections JOIN bins_detections ON detections.id = bins_detections.detections_id JOIN
                                   bins ON bins_detections.bins_id = bins.id JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON effort.id = bins_effort.effort_id
                                   WHERE detections.signal_code IN (5,6) AND detections.label = 1 AND detections.procedure = 10 AND
                                   effort.name ='fw_og_train'")

#alright, what others fgs do I want to use? I think that a hard negative and an odd positive are also in order here.
#"unusual": let's pull from high latitude, m5, un01, and KZ01. Sample, and make sure to manually add each
#of the mentioned sections in finproject/modelretrain.

#also want to do a hard negative pull. Do this like with LM- down to a certain threshold, and randomly within that.

#start with hard negatives since it's the easier sample. pull one that's half the size of bins_strat_sample1
#need to do this slightly different, since we need each bin to be both 0 for
fw2gen_hardneg_5 =dbGet("SELECT DISTINCT bins.id FROM bins JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN
                       detections ON bins_detections.detections_id = detections.id WHERE bins.type = 1 AND detections.signal_code = 5
                       AND detections.procedure =6 AND detections.label IN (0,20) AND detections.probability >= .95")

fw2gen_hardneg_6 =dbGet("SELECT DISTINCT bins.id FROM bins JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN
                       detections ON bins_detections.detections_id = detections.id WHERE bins.type = 1 AND detections.signal_code = 6
                       AND detections.procedure =6 AND detections.label IN (0,20) AND detections.probability >= .95")

#intersect isn't really that necessary- will favor the sections with airguns to much. Instead do union, then further subset this
#by querying for bins with y detections in these bins.
fw2gen_hardneg = union(as.integer(fw2gen_hardneg_5$id),as.integer(fw2gen_hardneg_6$id))

#use these bins and see which of them have any fin y detections (then remove)

fw2gen_hardneg_yeses = dbGet(paste("SELECT DISTINCT bins.id FROM bins JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN
                        detections ON bins_detections.detections_id = detections.id WHERE bins.type = 1 AND detections.signal_code IN (5,6)
                        AND detections.procedure = 6 AND detections.label IN (1,21) AND bins.id IN (",paste(fw2gen_hardneg,collapse=",",sep=""),")",sep=""))

#remove these from hardneg

fw2gen_hardneg = fw2gen_hardneg[-which(fw2gen_hardneg %in% as.integer(fw2gen_hardneg_yeses$id))]

fw2gen_hardneg_airguns = dbGet(paste("SELECT DISTINCT bins.id FROM bins JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN
                        detections ON bins_detections.detections_id = detections.id WHERE bins.type = 1 AND detections.signal_code = 21
                        AND detections.procedure = 8 AND detections.label IN (1,21) AND bins.id IN (",paste(fw2gen_hardneg,collapse=",",sep=""),")",sep=""))

fw2gen_hardneg = fw2gen_hardneg[-which(fw2gen_hardneg %in% as.integer(fw2gen_hardneg_airguns$id))]

#get the bin ids for previous 2 samples.

fw_og_train_bins = dbGet("SELECT bins_id FROM bins_effort JOIN effort ON effort.id = bins_effort.effort_id WHERE effort.name = 'fw_og_train'")
fw_og_train_bins = as.integer(fw_og_train_bins$bins_id)

strat_sample1_bins = dbGet("SELECT bins_id FROM bins_effort JOIN effort ON effort.id = bins_effort.effort_id WHERE effort.name IN
                           ('fw2gen_strat_pos_fewfin1','fw2gen_strat_pos_somefin1','fw2gen_strat_pos_manyfin1')")

strat_sample1_bins = as.integer(strat_sample1_bins$bins_id)

#don't run this, since there is no intersection here...
#fw2gen_hardneg = fw2gen_hardneg[-which(fw2gen_hardneg %in% union(fw_og_train_bins,strat_sample1_bins))]

#downsample: make it a 3rd of the latest size. Can always add another one if super quick or need more.
fw2gen_hardneg =sample(fw2gen_hardneg,250)

#load to db:

effort_table = data.frame("fw2gen_hardneg_ds",
                          "hard negative downsampled",
                          "Hard negatives from LM 1st gen detector deployment. >=.95 probability, and downsampled to flat 250 bins (3rd of strat_sample1 (fewfin1,somefin1,manyfin1)). Does not contain duplicates from original or strat_sample1"
)
colnames(effort_table) = c('name','sampling_method','description')

#dbAppendTable(con,'effort',effort_table)

#get new id

db_eff_row = dbGet("SELECT * FROM effort WHERE name = 'fw2gen_hardneg_ds'")

effort_procedures = data.frame(as.integer(db_eff_row$id),10,4,"train_eval","i_neg","n")
colnames(effort_procedures) = c("effort_id","procedures_id","signal_code","effproc_type","effproc_assumption","completed")

#dbAppendTable(con,'effort_procedures',effort_procedures)

allbins = data.frame(fw2gen_hardneg,as.integer(db_eff_row$id))

colnames(allbins) = c("bins_id","effort_id")

dbAppendTable(con,'bins_effort',allbins)

#good stuff. Time to start boxing again.

#turns out, this was almost all airguns. Redo, and need to add a step where I filter out airguns from the hardneg samples!
#try again- this time want to explicitly filter out airguns. Could make a seperate fg for airguns later (as needed..?)
#dbSendStatement(con,"DELETE FROM bins_effort WHERE effort_id = 284")

#that worked better, but signals were a little sparse- if I end up needing to include more, I should run a query where >1 calls
#are present per bin_id.

#think before going forward, it may be a good time to begin training and see what initial performance is looking like.
#for that, need to figure out either how to include multiple signal codes in formatGT (new vers?) or, load in 4 detections
#to db using the copied above process.

#start to get together referenced files from modelretrain.csv

#think what I will do, is gather like 10 bins from each (not included in
#any other sample that will be used.)

#struck

sites = c("UN01","KZ01","KZ01","KZ01",
          "KZ01","BS03","UN01",
          "PM08","PM05","PM05","PM05",
          "PM05","PM05","PM05","PM05",
          "PM02","PM02","UN01","PM02","PM05")
datetimes = c("140317-154000","140803-091000",
              "151114-130000","160901-011000",
              "160919-140000",
              "130331-221000","120113-210347",
              "140713-121000","110108-102000",
              "111007-142000","111012-132000",
              "111101-120000","111205-164001",
              "111228-102000","170817-230000",
              "210228-202000","210311-015000",
              "201225-123000","080309-143913",
              "091217-080000")

fw1difficult = data.frame(sites,datetimes)

fw1difficult$datetimes = as.POSIXct(fw1difficult$datetimes,format = "%y%m%d-%H%M%S")

#others, entire moorings were more implicated, like BS07_HA_02b and
#2009. For these, I may want to sample from a few yes sections in the detector
#results.




#also: want an fg composed of just yeses from higher latitude moorings
#> CL1

#this one is probably easier to query, so start with this.

fw2gen_oddtp = dbGet(
  "SELECT DISTINCT bins.id FROM detections JOIN bins_detections ON bins_detections.detections_id = detections.id
  JOIN bins ON bins.id = bins_detections.bins_id JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN
  data_collection ON data_collection.id = soundfiles.data_collection_id WHERE bins.type = 1 AND detections.procedure = 6 AND
  detections.label IN (1,21) AND data_collection.latitude > (SELECT MAX(latitude) FROM data_collection WHERE location_code = 'CC02')"
)

#remove any duplicates:
#taken bins =

taken_bins = dbGet("SELECT bins.id FROM bins JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON bins_effort.effort_id = effort.id
                    WHERE effort.name IN ('fw_og_train','fw2gen_hardneg_ds','fw2gen_strat_pos_fewfin1','fw2gen_strat_pos_somefin1',
                    'fw2gen_strat_pos_manyfin1')")

#check for duplicates- some found
any(as.integer(fw2gen_oddtp$id) %in% as.integer(taken_bins$id))

#remove them.
oddtp_bins = as.integer(fw2gen_oddtp$id)
oddtp_bins = oddtp_bins[-which(oddtp_bins %in% as.integer(taken_bins$id))]

#now can declare fg.

effort_table = data.frame("fw2gen_oddtp",
                          "procedural",
                          "All yes bins from latitude over CC02 in OG detector deployment. Does not contain duplicates from original,strat_samples1 (few/some/many), and hard_neg."
)
colnames(effort_table) = c('name','sampling_method','description')

#dbAppendTable(con,'effort',effort_table)
#look up id, hardcode below
effort_table$id = 285

effort_procedures = data.frame(effort_table$id,10,4,"train_eval","i_neg","n")
colnames(effort_procedures) = c("effort_id","procedures_id","signal_code","effproc_type","effproc_assumption","completed")
#dbAppendTable(con,'effort_procedures',effort_procedures)

bins = data.frame(oddtp_bins,285)
colnames(bins) = c("bins_id","effort_id")

#dbAppendTable(con,'bins_effort',bins)

#speed up the boxing a little bit- copy the fn detections, and reclassify them as procedure 10 and label 1.

oddtpfndets = dbGet(paste("SELECT DISTINCT detections.* FROM detections JOIN bins_detections on bins_detections.detections_id = detections.id
                            JOIN bins on bins.id = bins_detections.bins_id
                            WHERE detections.status = 1 AND detections.signal_code = 5 AND label IN (0,1,21) AND
                            procedure IN (6,10) AND bins.id IN (",paste(oddtp_bins,collapse=",",sep=""),")",sep=""))

oddtpfndets_change = oddtpfndets

oddtpfndets_change$id=NULL
oddtpfndets_change$probability=NA
oddtpfndets_change$comments = ""
oddtpfndets_change$modified = NULL
oddtpfndets_change$signal_code = 4
oddtpfndets_change$label = 1
oddtpfndets_change$analyst=NULL
oddtpfndets_change$status=NULL
oddtpfndets_change$procedure= 10
oddtpfndets_change$original_id = NULL

#dbAppendTable(con,'detections',oddtpfndets_change)


#now, need to define and work on last fg.
#how to query a 'buffer' for each region of interest?
#I could possibly reduce my timestamp down to hour, and
#retrieve that way. Yeah, that is probably the move.

#honestly, it may just be best to query in a loop.
#before I design query, I should identify a candidate sound file of BS07_HA_02b and 'M5 2009' (BS09_AU_PM05 ?)


#when selecting- I want to see the breakdown of month representation.

bins_w_months = dbGet("SELECT bins.id,date_trunc('month',soundfiles.datetime) FROM bins JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN effort ON bins_effort.effort_id = effort.id
                     JOIN soundfiles ON bins.soundfiles_id = soundfiles.id
                     WHERE effort.name IN ('fw_og_train','fw2gen_hardneg_ds','fw2gen_strat_pos_fewfin1','fw2gen_strat_pos_somefin1',
                    'fw2gen_strat_pos_manyfin1','fw2gen_oddtp')")

#hist(as.integer(format(bins_w_months$date_trunc,"%m")))
#looking at histogram, february seems like a good month to pull from as does november.

context_all = vector('list',nrow(fw1difficult))
for(i in 1:nrow(fw1difficult)){

  #get the hour where the file occurred and one hour after.
  hour = as.POSIXct(format(fw1difficult$datetime[i],"%y%m%d-%H"),format="%y%m%d-%H",tz = "")
  #hours = hour
  hours = c(hour,hour + 3600)

  #pull all bins from those hours, with that

  context = dbGet(paste("SELECT bins.id FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON
                   data_collection.id = soundfiles.data_collection_id WHERE date_trunc('hour',soundfiles.datetime) IN
                   ('",paste(hours,sep="",collapse="','"),"') AND data_collection.location_code ='",fw1difficult$sites[i],"'
                   AND bins.type = 1",sep=""))
  context_all[[i]] = as.integer(context$id)
}

context_all= do.call('c',context_all)

#remove duplicate bins
context_all = context_all[-which(context_all %in% as.integer(bins_w_months$id))]

effort_table = data.frame("fw1difficult",
                          "hand",
                          "opportunistic sections identified where fw1gen model struggled. Does not contain duplicates from original,strat_samples1 (few/some/many), and hard_neg, or oddtp"
)
colnames(effort_table) = c('name','sampling_method','description')

#dbAppendTable(con,'effort',effort_table)

#get id

id_ = dbGet("SELECT id FROM effort WHERE name = 'fw1difficult'")

effort_table$id = 286

effort_procedures = data.frame(effort_table$id,10,4,"train_eval","i_neg","n")
colnames(effort_procedures) = c("effort_id","procedures_id","signal_code","effproc_type","effproc_assumption","completed")
#dbAppendTable(con,'effort_procedures',effort_procedures)

bins = data.frame(context_all,286)
colnames(bins) = c("bins_id","effort_id")

#dbAppendTable(con,'bins_effort',bins)

#ok, it is loaded up ready to analyze.

poptpfndets = dbGet(paste("SELECT DISTINCT detections.* FROM detections JOIN bins_detections on bins_detections.detections_id = detections.id
                            JOIN bins on bins.id = bins_detections.bins_id
                            WHERE detections.status = 1 AND detections.signal_code IN (5,6) AND label IN (0,1,21) AND
                            procedure IN (6,10) AND bins.id IN (",paste(context_all,collapse=",",sep=""),")",sep=""))

poptpfndets_change = poptpfndets

poptpfndets_change$id=NULL
poptpfndets_change$probability=NA
poptpfndets_change$comments = ""
poptpfndets_change$modified = NULL
poptpfndets_change$signal_code = 4
poptpfndets_change$label = 1
poptpfndets_change$analyst=NULL
poptpfndets_change$status=NULL
poptpfndets_change$procedure= 10
poptpfndets_change$original_id = NULL

#dbAppendTable(con,'detections',poptpfndets_change)

#mark fg as completed:
dbSendStatement(con,"UPDATE effort_procedures SET completed = 'y' WHERE effort_id = (SELECT id FROM effort WHERE name = 'fw1difficult')")

#now, going to do last pass through each, make sure each fg looks complete before starting training.
#fw1difficult: looks good.
#og train: looks good.
#fw2gen_oddtp: looks good- looks like we missed a couple bins before adding fgfill.
#fw2gen_hardneg_ds: looks good. changed my mind on a small section
#others- assume they are good, as they sampled from y fin .

#now want to see again- how distributed are the labels?

fw_map = bin_label_explore(con,'fw',plot_sds = 2)
plot(fw_map)

#create the layers:
fw1 = add_layer_ble(con,5,10,'orange',c("fw_og_train"))
fw2 = add_layer_ble(con,6,10,'orange',c("fw_og_train"))

ff1 = add_layer_ble(con,4,10,'red1',c("fw2gen_strat_pos_fewfin1"))
sf1 = add_layer_ble(con,4,10,'red3',c("fw2gen_strat_pos_somefin1"))
mf2 = add_layer_ble(con,4,10,'red4',c("fw2gen_strat_pos_manyfin1"))
fg4 = add_layer_ble(con,4,10,'yellow2',c("fw2gen_oddtp"))
fg5 = add_layer_ble(con,4,10,'yellow3',c("fw2gen_hardneg_ds"))
fg6 = add_layer_ble(con,4,10,'yellow4',c("fw1difficult"))

#original
plot(fw_map + fw1 + fw2)

#all new strat_pos
plot(fw_map + ff1 + sf1 + mf2)

#all new other
plot(fw_map + fg4 + fg5 + fg6)

#all:
plot(fw_map + fw1 + fw2 + ff1 + sf1 + mf2 + fg4 + fg5 + fg6 )

#cool- sampling looks pretty good!

#one thing to note- look into bs10_AU_05- did this just not get loaded
#up to db? not only does it look like it's not loaded up to db, but that
#the sfs themselves are not uploaded to the db.


#need a fxn to explore the 99 detections. I think a generalizable function to generate a plot which shows-
#1. daily % presence + shade coded detection density
#2. any number of named mooring deployments
#3. any number of individual procedures (color coded)
#4. argument to control uk behavior (turn 99s to 21s for sake of comparison..)
#5. monthly or daily time scales

deployments = c("AL20_AU_PH01","BS19_AU_PM05","BS20_AU_PM08","BS18_AU_PM02-a","BS18_AU_PM02-b","BS17_AU_PM08")
deployments = c("BS13_AU_PM02-a")
procedures = c(6,24)
bin_type= 1
timescale = 'month'
uk_behavior = 'include'
color_scale = 'log'
prob_thresh = 0.0

test6= perc_pres(con,deployments,procedures,bin_type,timescale,uk_behavior,color_scale,prob_thresh)
plot(test6)

#needs
#deal with overlapping name
#loop through each procedure to sum total bins (or do in query?)
perc_pres <- function(conn,deployments,procedures,bin_type,timescale = 'day',uk_behavior = 'ignore',color_scale='none',prob_thresh='off'){

    bt_str = c("LOW","REG","SHI","CUSTOM")[bin_type]


    #hardcode in to make sure behavior stays consistent, despite og fin whale data being
    #loaded in over two procedures (positive and negative as 6,7)



    if(6 %in% procedures){
      procedures = c(procedures,7)
    }

    bt_str = c("LOW","REG","SHI","CUSTOM")[bin_type]

    #query = paste("SELECT procedure, CASE WHEN probability > ",prob_thresh," THEN 'in_prob' WHEN probability IS NULL THEN 'no_prob' END as range, label,count(*),date_trunc('",timescale,"', soundfiles.datetime) AS dt_,data_collection.name,data_collection.location_code,data_collection.latitude
    #            FROM detections JOIN bins_detections ON bins_detections.detections_id = detections.id JOIN bins ON bins_detections.bins_id = bins.id
    #            JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
    #            WHERE bins.type = ",bin_type," AND data_collection.name IN ('",paste(deployments,collapse="','",sep=""),"') AND detections.procedure IN (",paste(procedures,collapse=",",sep=""),")
    #             GROUP BY procedure,range,label,dt_,data_collection.name,data_collection.location_code,data_collection.latitude",sep="")

    if(uk_behavior=='ignore'){
      label_behavior = "CASE WHEN label IN (21,1) THEN 1 WHEN label IN (20,0) THEN 0 END as label_"
    }else if(uk_behavior == 'include'){
      label_behavior = "CASE WHEN label IN (21,1,99) THEN 1 WHEN label IN (20,0) THEN 0 END as label_"
    }


    #need to check- do I need to add special CASE behavior for 6,7 procedures?

    query = paste("SELECT procedure, CASE WHEN probability >= ",prob_thresh," THEN 'in_prob' WHEN probability < ",prob_thresh," THEN 'out_prob' WHEN probability IS NULL THEN 'no_prob' END as range,count(DISTINCT bins.id) AS unq_bins,",label_behavior,",count(*),date_trunc('",timescale,"', soundfiles.datetime) AS dt_,data_collection.location_code,data_collection.latitude
                FROM detections JOIN bins_detections ON bins_detections.detections_id = detections.id JOIN bins ON bins_detections.bins_id = bins.id
                JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
                WHERE (probability >= ",prob_thresh," OR probability IS NULL) AND bins.type = ",bin_type," AND data_collection.name IN ('",paste(deployments,collapse="','",sep=""),"') AND detections.procedure IN (",paste(procedures,collapse=",",sep=""),") AND detections.status = 1
                 GROUP BY procedure,range,label_,dt_,data_collection.location_code,data_collection.latitude",sep="")

    output = dbFetch(dbSendQuery(conn,gsub("[\r\n]", "", query)))

    #don't need this anymore
    output$range = NULL

    colnames(output)[which(colnames(output)=="label_")] = 'label'

    if(any(is.na(output$label))){
      output = output[-which(is.na(output$label)),]
    }

    output_mod = output
    #output_mod$name = NULL

    if(6 %in% procedures){
      om7 = output_mod[which(output_mod$procedure==7),]
      om6 = output_mod[which(output_mod$procedure==6),]

      colnames(om7)[which(colnames(om7)=="unq_bins")]="unq_bins7"

      om7$procedure = 6
      om7$count = NULL

      om_temp = merge(om6,om7,all=TRUE)

      om_temp$unq_bins[which(is.na(om_temp$unq_bins))]=0
      om_temp$unq_bins7[which(is.na(om_temp$unq_bins7))]=0

      om_temp$unq_bins= om_temp$unq_bins+om_temp$unq_bins7
      om_temp$unq_bins7=NULL

      output_mod = output_mod[-which(output_mod$procedure %in% c(6,7)),]
      output_mod =rbind(output_mod,om_temp)
    }

    output_mod$count= as.integer(output_mod$count)

    #query2 = paste("SELECT count(*) as eff_count,date_trunc('",timescale,"', soundfiles.datetime)AS dt_,data_collection.name,data_collection.location_code,data_collection.latitude
    # FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON soundfiles.data_collection_id = data_collection.id
    # WHERE bins.type = ",bin_type," AND data_collection.name IN ('",paste(deployments,collapse="','",sep=""),"')
    # GROUP BY dt_,data_collection.name,data_collection.location_code,data_collection.latitude",sep="")

    #bins_in_ts = dbFetch(dbSendQuery(conn,gsub("[\r\n]", "", query2)))

    #bins_in_ts$eff_count= as.integer(bins_in_ts$eff_count)

    #comb= merge(output_mod,bins_in_ts, all.y = TRUE)

    eff_table = output_mod[which(output_mod$label==0),]
    det_table = output_mod[which(output_mod$label==1),]

    colnames(eff_table)[which(colnames(eff_table)=="unq_bins")]="no_unq_bins"

    eff_table$count= NULL
    eff_table$label = NULL

    comb = merge(det_table,eff_table,all = TRUE)

    comb$unq_bins[which(is.na(comb$unq_bins))]=0
    comb$no_unq_bins[which(is.na(comb$no_unq_bins))]=0

    comb$total_bins = comb$unq_bins + comb$no_unq_bins
    comb$perc_pres = comb$unq_bins/comb$total_bins

    comb$procedure = as.factor(comb$procedure)

    if(color_scale=='log'){
      comb$count = log(comb$count)
    }else if(color_scale=='sqrt'){
      comb$count = sqrt(comb$count)
    }

    #now plot

    base_map =ggplot(comb, aes(dt_, perc_pres)) +
      geom_line(aes(color = procedure,alpha =count)) +
      scale_alpha(range = c(0.35, 1)) +
      scale_y_continuous(limits=c(0, 1), expand = c(0, 0)) +
      scale_x_datetime(date_labels = "%m-%y",expand=c(0,0)) +
      ggtitle(paste(bt_str,timescale,"bin % presence")) +
      facet_grid(rows = vars(location_code )) +
      theme_bw() +
      theme(legend.title = element_text(size=12),
            legend.text = element_text(size=12)) +
      theme(axis.text = element_text(size=12),
            axis.title.y = element_blank()) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
      labs(fill = "% presence")

    return(base_map)
}


