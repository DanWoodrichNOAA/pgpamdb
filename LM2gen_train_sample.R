#create the fgs for the lm2gen detector.
#three will be non-overlapping, from original training, rand i_neg, and pos i_neg
#other two will be hard negatives and obcscure positives.

#don't load this in, since I might use to make changes to package.
#library(pgpamdb)
#load in the library the more direct way.

library(RPostgres)
library(foreach)
library(tuneR)

#setwd("C:/Apps/pgpamdb")

source("./R/functions.R") #package under construction

source("./etc/paths.R") #populates connection paths which contain connection variables.

#as I use this script, I'm just deleting and changing code to create the lookup table. to upload fgs only, I only use the first
#part of the loop below

con=pamdbConnect("poc_v2",keyscript,clientkey,clientcert)

#cut down on keystrokes
dbGet <-function(x){
  x = gsub("[\r\n]", "", x)
  dbFetch(dbSendQuery(con,x))
}

#pull in the 3 existing fg to use:

#this one is a join of all the existing lm_fgs.
lm_1genfgs = dbGet("SELECT id FROM effort WHERE name LIKE '%lm%'")

lm2gen_og_train = dbGet("SELECT bins.id FROM bins JOIN bins_effort ON bins_effort.bins_id = bins.id JOIN
                        effort ON effort.id = bins_effort.effort_id WHERE effort.id IN (45,48,49,50,46,47)")

lm2gen_og_train = as.integer(lm2gen_og_train$id)

#great. Now, load in lmpos train.
lm2gen_train_pos_set_no_olvp = dbGet("SELECT bins_id FROM bins_effort WHERE effort_id = 76") #LMyesSample_1

lm2gen_train_pos_set_no_olvp = as.integer(lm2gen_train_pos_set_no_olvp$bins_id)

lm2gen_train_pos_set_no_olvp = lm2gen_train_pos_set_no_olvp[-which(lm2gen_train_pos_set_no_olvp %in% lm2gen_og_train)]

#great. now load in lmneg train

lm2gen_train_rand_set_no_ovlp = dbGet("SELECT bins_id FROM bins_effort WHERE effort_id = 104") #oneHRonePerc

lm2gen_train_rand_set_no_ovlp = as.integer(lm2gen_train_rand_set_no_ovlp$bins_id)

lm2gen_train_rand_set_no_ovlp = lm2gen_train_rand_set_no_ovlp[-which(lm2gen_train_rand_set_no_ovlp %in% c(lm2gen_train_pos_set_no_olvp,lm2gen_og_train))]

#great. those are all taken care of. Now, need to load in bins corresponding to 'hard negatives'.

lm_allprobs = dbGet("SELECT probability,label FROM detections WHERE procedure = 5 AND probability IS NOT NULL")

#view relationship

hist(lm_allprobs$probability[which(lm_allprobs$label==0)],col=rgb(0,0,1,1/2),xlab = "probability",main= NULL)
hist(lm_allprobs$probability[which(lm_allprobs$label==1)],col=rgb(1,0,0,1/2),add=TRUE)

#based on this, I would start with .95 and higher as high prob- but, need to see how many bins I end up with
#should not exceed 750 non overlapping for time reasons...

lm2gen_hardneg =dbGet("SELECT DISTINCT bins.id FROM bins JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN
                      detections ON bins_detections.detections_id = detections.id WHERE bins.type = 1 AND detections.procedure= 5
                      AND detections.label = 0 AND detections.probability >= .95")

lm2gen_hardneg = as.integer(lm2gen_hardneg$id)

length(lm2gen_hardneg[-which(lm2gen_hardneg %in% c(lm2gen_train_rand_set_no_ovlp,lm2gen_train_pos_set_no_olvp,lm2gen_og_train))])

#.95 cutoff resulted in 6071 returned rows- I could either move up the probability threshold, or I could further sample
#from this.

#I am partial to further random sampling of this selection. Reason being, is that I expect the ultra high values to be
#less variable in the signals they represent.

lm2gen_hardneg = lm2gen_hardneg[-which(lm2gen_hardneg %in% c(lm2gen_train_rand_set_no_ovlp,lm2gen_train_pos_set_no_olvp,lm2gen_og_train))]

#downsample this
lm2gen_hardneg_ds = sample(lm2gen_hardneg,round(length(lm2gen_hardneg)/3))


#great. Now for the last bit- unusual positives. This one will require accessing spatial info as well.

#first, let's understand where the positives are coming from.
#to avoid the bias effect where we are more likely to apply the detector to the suspected source of lm,
#we compare tp vs fp rates in the different locations.

#so the pseudo is- pull in count(tp)/count(fp), grouped by location_code

tp_rate = dbGet("SELECT location_code, COUNT(CASE WHEN label = 1 THEN 1 END) AS tp,
                 COUNT(CASE WHEN label = 0 THEN 1 END) AS fp,
                 COUNT(DISTINCT(data_collection.name)) AS dep_num
                 FROM detections JOIN soundfiles
                 ON detections.start_file = soundfiles.id JOIN data_collection ON
                 data_collection.id = soundfiles.data_collection_id WHERE detections.procedure = 5 GROUP BY location_code")

tp_rate$rate = tp_rate$tp/(tp_rate$tp + tp_rate$fp)

tp_rate$per_dep = tp_rate$tp/tp_rate$dep_num

#the rate is a little funky given some low sample sizes- instead, lets just reject on counts of tp
#lets say, reject locations with over 100 tps.
rejects = tp_rate[which(tp_rate$per_dep>100),"location_code"] # "BS03" "PM02" "PM04"

#just for a fun look into the data, I kind of want to include a calculation of duration for these- see how it
#changes over more or less certain sites.

tp_rate_plus_dur = dbGet("SELECT location_code, COUNT(CASE WHEN label = 1 THEN 1 END) AS tp,
                 COUNT(CASE WHEN label = 0 THEN 1 END) AS fp, AVG(detections.end_time - detections.start_time) as avg_dur,
                 COUNT(DISTINCT(data_collection.name)) AS dep_num FROM detections JOIN soundfiles
                 ON detections.start_file = soundfiles.id JOIN data_collection ON
                 data_collection.id = soundfiles.data_collection_id WHERE detections.procedure = 5
                         AND detections.start_file = detections.end_file AND detections.label = 1 GROUP BY location_code")

View(tp_rate_plus_dur) #looks pretty stable!

#alright, moving along- this query will pull unique bin_ids from detections which are not from the
#excluded site ids. Shoot for about 750 bins again.

lm2gen_oddtp = dbGet(
  "SELECT DISTINCT bins.id FROM detections JOIN bins_detections ON bins_detections.detections_id = detections.id
  JOIN bins ON bins.id = bins_detections.bins_id JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN
  data_collection ON data_collection.id = soundfiles.data_collection_id WHERE bins.type = 1 AND detections.procedure = 5 AND
  detections.label = 1 AND data_collection.location_code NOT IN ('BS03','PM02','PM04')"
)

lm2gen_oddtp = as.integer(lm2gen_oddtp$id)

#remove duplicates:

lm2gen_oddtp = lm2gen_oddtp[-which(lm2gen_oddtp %in% c(lm2gen_train_pos_set_no_olvp,lm2gen_train_rand_set_no_ovlp,lm2gen_hardneg_ds,lm2gen_og_train))]


#downsample by a third:

#lm2gen_oddtp = sample(lm2gen_oddtp,round(length(lm2gen_oddtp)/3))


#quick check- all lm data are in low, right?
#they are!
dbGet("SELECT COUNT(bins.type) FROM bins JOIN bins_effort ON bins.id = bins_effort.bins_id JOIN
      effort ON bins_effort.effort_id = effort.id WHERE effort.name LIKE ('%lm%') OR effort.name IN ('LMyesSample_1','oneHRonePerc') GROUP BY bins.type")#

#great- so now, declare and populate the new fgs.
#need entry in effort, effort_procedures, then in bins_effort.

effort_table = data.frame(c("lm2gen_og_train","lm2gen_train_pos_set_no_olvp","lm2gen_train_rand_set_no_ovlp","lm2gen_hardneg_ds","lm2gen_oddtp_ds"),
                          c("semi-random no overlap","high grade random no overlap","random no overlap","hard negative downsampled","procedural"),
                          c("The ground truth training used in the previous LM detector: BS13_AU_PM02-a_files_343-408_lm, BS14_AU_PM04_files_189-285_lm, BS14_AU_PM04_files_304-430_lm, BS14_AU_PM04_files_45-188_lm, BS13_AU_PM02-a_files_38-122_lm, BS13_AU_PM02-a_files_510-628_lm",
                            "LMyesSample_1 without overlap from lm2gen_og_train (1st gen training set)",
                            "oneHRonePerc without overlap from lm2gen_og_train (1st gen training set) OR lm2gen_train_pos_set_no_olvp",
                            "Hard negatives from LM 1st gen detector deployment. >=.95 probability, and downsampled by factor of 9. No overlap with lm2gen_og_train, lm2gen_train_pos_set_no_olvp, lm2gen_train_rand_set_no_ovlp",
                            "True positives from less common sites (minus all with > 100 LM tp/mooring deployment avg: BS03 PM02 PM04) from LM 1st gen deployment. no overlap with lm2gen_og_train, lm2gen_train_pos_set_no_olvp, lm2gen_train_rand_set_no_ovlp, or lm2gen_hardneg_ds"))

colnames(effort_table) = c('name','sampling_method','description')

dbAppendTable(con,'effort',effort_table)

effort_table$id = 248:252

#great, now make a table for effort_procedures

effort_procedures = data.frame(effort_table$id,21,3,"train_eval","i_neg",c("y","y","y","n","n"))
colnames(effort_procedures) = c("effort_id","procedures_id","signal_code","effproc_type","effproc_assumption","completed")

dbAppendTable(con,'effort_procedures',effort_procedures)

#now, need to make the table of bins+ effort id.
allbins = list()

allbins[[1]] = data.frame(lm2gen_og_train,248)
colnames(allbins[[1]]) = c("bins_id","effort_id")

allbins[[2]] = data.frame(lm2gen_train_pos_set_no_olvp,249)
colnames(allbins[[2]]) = c("bins_id","effort_id")

allbins[[3]] = data.frame(lm2gen_train_rand_set_no_ovlp,250)
colnames(allbins[[3]]) = c("bins_id","effort_id")

allbins[[4]] = data.frame(lm2gen_hardneg_ds,251)
colnames(allbins[[4]]) = c("bins_id","effort_id")

allbins[[5]] = data.frame(lm2gen_oddtp_ds,252)
colnames(allbins[[5]]) = c("bins_id","effort_id")

allbins = do.call("rbind",allbins)

#perfect, no duplicates.

#so, can load this to db.

dbAppendTable(con,'bins_effort',allbins)

#alright, so the last two fgs were messed up since I didn't mandate that the bin type was 1.

#So- remove 251 and 252 from db:

dbGet("DELETE FROM bins_effort WHERE effort_id IN (251,252)")

#now supply fixed detections.

allbins = list()

allbins[[1]] = data.frame(lm2gen_hardneg_ds,251)
colnames(allbins[[1]]) = c("bins_id","effort_id")

allbins[[2]] = data.frame(lm2gen_oddtp,252)
colnames(allbins[[2]]) = c("bins_id","effort_id")

allbins = do.call("rbind",allbins)

dbAppendTable(con,'bins_effort',allbins)



#made rest of changes to information on db.





#perfect, no they are in in. Check the ones which are in, to make sure we aren't getting duplicates.
#how will this all work? I will have to load in all dets- but, this will still mean that all of the procedure
#will still show up where they are duplicated within a single fg. How to combat this?

#have a hard_coded gt method to screen out based on fg names... yuck

#make it normal protocol to change detections which were originally loaded up under a certain procedure to '10' - heavy, less transparent

#have a behavior in format gt where detected 'overlapping' detections are removed. Guess at the
#important one based on counts...? This is assign labels all over again, super gross.
#however, taking a superset would be nice for training purpose. if it's the same signal code, only would
#need to compare across time. This is probably the best way to go...! However, perhaps makes it harder to
#backtrace to original ds.

#probably the way to explore. should be a parameter - remove_proc_dups

#give this a try.




