#this package is pretty loosely maintained at the moment, there are some functions in 
#various stages of depreciation and I've found it hard to stick to a data model as my
#work evolves. That said, a few are useful/interesting, and always happy to add ideas!

#####Dependencies: 

#make sure the following packages are installed
#install_packages("RPostgres")
#install.packages("//nmfs/akc-nmml/CAEP/Acoustics/Matlab Code/Other code/R/pgpamdb/pgpamdb_0.1.24.tar.gz", source = TRUE, repos=NULL)

#I usually hard code this in as a quick way for SELECT queries, makes it so you don't have to
#consider string formatting, and specify 'con' a million times. Just shorthand for casual use.
dbGet <-function(x){
  x = gsub("[\r\n]", "", x)
  dbFetch(dbSendQuery(con,x))
}

library(pgpamdb)
library(RPostgres)

#####DB credentials

#I use an R file to define the below three variables (helps obscure where local sensitive files are,
#which is especially relevant if you are publishing to public repos but usually not too dangerous):
#source("./etc/paths.R") 

#Otherwise, you can also do this:


keyscript = "example/path/file.R" #path to .R file that contains definitions for dsn_hostname,dsn_port,dsn_uid, and dsn_pwd (provided to you in chat)
#keep this file far away from your code as it contains a password. contents of file should be to:
#dsn_hostname = "10.27.15.3"
#dsn_port = 5432
#dsn_uid = "Dana"
#dsn_pwd = ""

clientkey = "example/path/client-cert.pem" #also provided in chat
clientcert = "example/path/client-key.pem" #also provided in chat

con=pgpamdb::pamdbConnect("poc_v3",keyscript,clientkey,clientcert)

#########WARNING

#FORBIDDEN functions to very much *not* try before I give you a little more db training
#(convenience fxns to insert, delete or modify data on db)

#you should not be able to modify data currently, but probably best not even to attempt yet

#pgpamdb::?i_neg_update()
#pgpamdb::?table_insert()
#pgpamdb::?table_update()
#pgpamdb::?table_delete()
#pgpamdb::?load_soundfile_metadata()


#########functions you can try safely:

#querying functions:
#?lookup_from_match(): get an id column from a provided vector. 
lookup_from_match(con,'personnel',c("Dan","Dana"),match_col = "pg_name")

#pgpamdb::?table_dataset_lookup(): #kind of a clunky one in terms of arguments, but allows you to stick on metadata sourced from the db to an existing R dataset based on matching column values.
#ex:

#say you have a dataset of pngs that you got from catherine and want to associate db data
test = read.csv(text="
MoorDeployPath	MoorDeploy	StartPngSec	EndPngSec
AU-ALBF02-161006-021000.wav	AL16_AU_BF02  540	600
AU-ALBF02-161006-030000.wav	AL16_AU_BF02	270	360
AU-ALBF02-161006-121000.wav	AL16_AU_BF02	180	270
AU-ALBF02-161006-121000.wav	AL16_AU_BF02	270	360
AU-ALBF02-161006-121000.wav	AL16_AU_BF02	360	450
AU-ALBF02-161006-121000.wav	AL16_AU_BF02	450	540
",sep="\t",header=TRUE)

#if you reformat the names to match the queries, and supply the postgres data types, you can build a query from that.
#Here is an example where you pull out all of the analysts who have worked on this data.

#processing step: change to db names: 
colnames(test)= c("soundfiles.name","data_collection.name","bins.seg_start","bins.seg_end")

analysts_in_sample = table_dataset_lookup(con,"SELECT DISTINCT personnel.name FROM data_collection JOIN soundfiles ON data_collection.id = soundfiles.data_collection_id JOIN bins ON bins.soundfiles_id = soundfiles.id JOIN bins_detections ON bins.id = bins_detections.bins_id JOIN detections ON bins_detections.detections_id = detections.id JOIN personnel ON detections.analyst = personnel.id",
                     test,c("CHARACTER VARYING","CHARACTER VARYING","DOUBLE PRECISION","DOUBLE PRECISION"),return_anything = TRUE)

#visualization functions:
#pgpamdb::?bin_label_explore() #plot at high level presence of data on db. 
#Same idea as the dashboard https://10.27.25.170/mml_acoustics_dash/

lm_map = bin_label_explore(con,'LM') #grab your popcorn: large, complex, long-running query... 
#more convenient to access this info on the dashboard, since results are cached. 

plot(lm_map)

#add a layer on  the above, which I like to do sometimes to visualize where I have sourced ground truth data 
#pgpamdb::?bin_label_explore()

lm_map_w_cole_rand_set = lm_map + add_layer_ble(con,3,12,'blue',c("lm2gen_train_rand_set_no_ovlp"))
plot(lm_map_w_cole_rand_set)

#visualization functions could be a lot more fleshed out!

#########customized querying with SQL:

#most powerful/flexible tool: direct querying with SQL. You can basically grab whatever you want with this, then plot it in R. 
#Go ahead and read anything you want with SELECT (SQL), and please attempt to run nothing including CREATE, INSERT, UPDATE, or DELETE (DML)

#some examples, slowly building complexity:

#get 5 detections
data1 = dbGet("SELECT * FROM detections LIMIT 5")
#get 5 current right whale upcall 'yes' detections  
data2 = dbGet("SELECT * FROM detections JOIN signals ON detections.signal_code= signals.id JOIN label_codes ON detections.label = label_codes.id 
              WHERE signals.soundchecker_name = 'right' AND label_codes.name = 'yes' and status = 1 LIMIT 5")
#get 5 regular bins which contain a current, yes right whale detection. This time, just use the ids on detections to avoid the join to signal and label lookup tables. 
data3 = dbGet("SELECT bins.*,soundfiles.name FROM bins JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN detections ON detections.id = bins_detections.detections_id
              JOIN soundfiles ON soundfiles.id = bins.soundfiles_id
              WHERE detections.signal_code = 1 AND detections.status = 1 AND label = 1 LIMIT 5")
#get top 5 most dense soundfiles containing right whale, and the total counts in each
data4 = dbGet("SELECT soundfiles.name,COUNT(*) AS total_upcalls FROM soundfiles JOIN bins ON soundfiles.id = bins.soundfiles_id JOIN bins_detections ON bins_detections.bins_id = bins.id 
              JOIN detections ON detections.id = bins_detections.detections_id
              WHERE detections.signal_code = 1 AND detections.status = 1 AND label = 1 GROUP BY soundfiles.name ORDER BY total_upcalls DESC LIMIT 5")

#you can also do very fancy stuff: here is a query that finds all intersections of fin whale calls and low moans overlapping by both time and frequency in the mooring BS13_AU_PM02-a:
#You can use INSTINCT/Raven integration to view the outputs on real data of any query you can dream up, but that currently is not accessible through this package
#(query to raven file- maybe in the future!)

fancy_query = "WITH fwsub AS (SELECT detections.id,low_freq,high_freq,soundfiles.data_collection_id,s1.datetime + interval '1 second' * detections.start_time AS datetime_start, 
         s2.datetime + interval '1 second' * detections.end_time as datetime_end 
         from detections JOIN soundfiles AS s1 ON detections.start_file = s1.id 
         JOIN soundfiles as s2 ON detections.end_file = s2.id 
         JOIN soundfiles ON detections.start_file = soundfiles.id 
         JOIN data_collection ON data_collection.id = soundfiles.data_collection_id 
         WHERE data_collection.name = 'BS13_AU_PM02-a' AND signal_code IN (4,5,6) AND label IN (1,21)), lmsub AS
         (SELECT detections.id,low_freq,high_freq,soundfiles.data_collection_id,s1.datetime + interval '1 second' * detections.start_time AS datetime_start, 
         s2.datetime + interval '1 second' * detections.end_time as datetime_end 
         FROM detections JOIN soundfiles AS s1 ON detections.start_file = s1.id 
         JOIN soundfiles as s2 ON detections.end_file = s2.id 
         JOIN soundfiles ON detections.start_file = soundfiles.id 
         JOIN data_collection ON data_collection.id = soundfiles.data_collection_id 
         WHERE data_collection.name = 'BS13_AU_PM02-a' AND procedure = 5 AND label = 1)
         SELECT detections.* FROM detections JOIN fwsub ON detections.id = fwsub.id 
         WHERE EXISTS (SELECT 1 FROM lmsub 
           WHERE tstzrange(lmsub.datetime_start, lmsub.datetime_end, '()') &&
           tstzrange(fwsub.datetime_start, fwsub.datetime_end, '()') AND 
           numrange(lmsub.low_freq::numeric, lmsub.high_freq::numeric) &&
           numrange(fwsub.low_freq::numeric, fwsub.high_freq::numeric)
           AND fwsub.data_collection_id = lmsub.data_collection_id) 
         UNION SELECT detections.* FROM detections JOIN lmsub ON detections.id = lmsub.id WHERE EXISTS (SELECT 1 FROM fwsub 
           WHERE tstzrange(fwsub.datetime_start, fwsub.datetime_end, '()') 
           && tstzrange(lmsub.datetime_start, lmsub.datetime_end, '()') AND 
           numrange(fwsub.low_freq::numeric, fwsub.high_freq::numeric) &&
           numrange(lmsub.low_freq::numeric, lmsub.high_freq::numeric)
           AND fwsub.data_collection_id = lmsub.data_collection_id)" 

data5 = dbGet(fancy_query) #again, grab your popcorn

#also, note that R string manipulation can be used to build more dynamic queries. Sometimes you will hit limits on query
#size, at which point there are tricks you can use to send the data in batches on the backend. 

ids = c(1,2,3,4,5,6,7,8,9)

#get all of the procedures which correspond to your vector of IDs
data6 = dbGet(paste("SELECT * FROM procedures WHERE id IN (",paste(ids,collapse=",",sep=""),")",sep=""))

#try your own!
