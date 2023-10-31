
dbGet <-function(x){
  x = gsub("[\r\n]", "", x)
  dbFetch(dbSendQuery(con,x))
}

library(pgpamdb)
library(DBI)

#pull template
source("./etc/paths.R")
con=pamdbConnect("poc_v3",keyscript,clientkey,clientcert)

#plan for ribbon training:
#use old training data
#use outputs from new data. Tightly bound the calls, and create new
#bins for these calls which will be used as the fg. pull both
#positive and negative calls.
#using the above strategy, perhaps break the calls into groups based
#on signal density (as with fins)

#initially- want to explore where ribbon calls are represented in the data.
#for that, we will want to revise the package function to show this as
#we did for LM and FW.

#work on it in here, then move it to fxns.
conn = con
bin_label = "RB"
inst_source='AFSC'
plot_sds = 4
bin_type=3


bin_label_explore<-function(conn,bin_label,inst_source='AFSC',plot_sds = 4,bin_type=1){

  bt_str = c("LOW","REG","SHI","CUSTOM")[bin_type]
  #bin_label= 'fw' #temp
  #inst_source = "AFSC"

  if(inst_source == "AFSC"){
    #bound the approximate region to get rid of the GR and other non-region AFSC data.
    long_min = -185
    long_max = -140

    #moved this up here, so that query will not reject sites without precise latlong info. Will pull all of the averages
    #for each site, then restrict by site in below query, not by

    query_ = "SELECT AVG(latitude) as avg_lat,AVG(longitude) as avg_long,location_code FROM data_collection GROUP BY location_code"

    avg_latlongs = dbFetch(dbSendQuery(conn,gsub("[\r\n]", "", query_)))

    avg_latlongs = avg_latlongs[which(!is.na(avg_latlongs$avg_lat)),]

    in_bounds_loc_codes = avg_latlongs[which(avg_latlongs$avg_long > long_min & avg_latlongs$avg_long < long_max),]

  }

  query = paste("SELECT ",bin_label,",count(*),date_trunc('month', soundfiles.datetime) AS dt_,data_collection.name,data_collection.location_code,data_collection.latitude
                FROM bin_label_wide JOIN bins ON bins.id = bin_label_wide.id JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
                WHERE bins.type = ",bin_type," AND data_collection.institution_source = '",inst_source,"' AND data_collection.location_code IN ('",paste(unique(in_bounds_loc_codes$location_code),sep="",collapse="','"),"')
                 GROUP BY ",bin_label,",dt_,data_collection.name,data_collection.location_code,data_collection.latitude",sep="")

  #make sure this is properly providing bin labels as opposed to detection labels!
  #right now, this is probably returning sums of detections categories within the months- not necessarily meaningful.
  #what will I have to do? Can I get query to create behavior or will I have to do a custom aggregation in the script?
  query = paste("SELECT detections.label,count(*),date_trunc('month', soundfiles.datetime) AS dt_,data_collection.name,data_collection.location_code,data_collection.latitude
                FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
                JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN detections ON bins_detections.detections_id = detections.id JOIN signals ON signals.id = detections.signal_code
                WHERE bins.type = ",bin_type," AND data_collection.institution_source = '",inst_source,"' AND data_collection.location_code IN ('",paste(unique(in_bounds_loc_codes$location_code),sep="",collapse="','"),"')
                AND signals.code = '",bin_label,"'
                GROUP BY detections.label,dt_,data_collection.name,data_collection.location_code,data_collection.latitude",sep="")

  #attempt to just get the most relevant label.
  #the idea: will just return the highest ranked case for each bins.id.
  #kind of crude: doesn't take into account label assumptions (loose vs tight),
  #conflicts. Not to be used for much outside of a quick visualization.
  #COUNT(CASE WHEN subquery.label IN (1,21) THEN 1 END) AS p_bins COUNT(CASE WHEN subquery.label IN (0,21) THEN 1 END) AS n_bins
#/ NULLIF(COUNT(CASE WHEN subquery.label IN (0,21) THEN 1 END)+COUNT(CASE WHEN subquery.label IN (1,21) THEN 1 END),0)
  query = paste("SELECT COUNT(CASE WHEN subquery.label IN (1,21) THEN 1 END)::decimal / NULLIF(COUNT(CASE WHEN subquery.label IN (0,20) THEN 1 END)+COUNT(CASE WHEN subquery.label IN (1,21) THEN 1 END),0) AS x_bins,
  subquery2.dt_,subquery2.location_code,subquery3.latitude FROM (SELECT DISTINCT ON (bins.id) detections.label,bins.id,COUNT(*),date_trunc('month', soundfiles.datetime) AS dt_,data_collection.location_code,
  CASE detections.label WHEN 1 THEN 1 WHEN 21 THEN 2 WHEN 0 THEN 3 WHEN 20 THEN 4 ELSE 5 END AS label_preference
  FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
  JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN detections ON bins_detections.detections_id = detections.id JOIN signals ON signals.id = detections.signal_code
  WHERE bins.type = ",bin_type," AND data_collection.institution_source = '",inst_source,"' AND data_collection.location_code IN ('",paste(unique(in_bounds_loc_codes$location_code),sep="",collapse="','"),"')
  AND signals.code = '",bin_label,"'
  GROUP BY detections.label,bins.id,dt_,data_collection.location_code
  ORDER BY bins.id,label_preference) AS subquery RIGHT JOIN (
                SELECT date_trunc('month', soundfiles.datetime) AS dt_,
                data_collection.location_code
                FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON
                data_collection.id = soundfiles.data_collection_id
                WHERE bins.type = ",bin_type," AND data_collection.institution_source = '",inst_source,"' AND data_collection.location_code IN ('",paste(unique(in_bounds_loc_codes$location_code),sep="",collapse="','"),"')
                GROUP BY dt_,data_collection.location_code) AS subquery2
                ON subquery.dt_ = subquery2.dt_ AND subquery.location_code = subquery2.location_code
                JOIN (SELECT AVG(latitude) as latitude, location_code FROM data_collection GROUP BY location_code) AS subquery3
                ON subquery2.location_code = subquery3.location_code GROUP BY subquery2.dt_,subquery2.location_code,subquery3.latitude",sep="")

  #

  #not working to solve the above with left joins, so instead, maybe I run a seperate query and
  #use it to get total bins for

  output = dbFetch(dbSendQuery(conn,gsub("[\r\n]", "", query)))

  scale_midpoint = mean(output$x_bins,na.rm=TRUE)
  scale_sd = sd(output$x_bins,na.rm=TRUE)

  scale_max = scale_midpoint+ scale_sd*plot_sds

  if(scale_max>1){
    scale_max = 1
  }

  output[which(output$x_bins>scale_max),"x_bins"]=scale_max

  colnames(output)[which(colnames(output)=="dt_")]="mm_yy"

  #order based on latitude
  output$location_code =factor(output$location_code,level = unique(output$location_code[order(output$latitude)]))

  base_map =ggplot(output, aes(mm_yy, location_code)) +
    geom_tile(aes(fill = x_bins), color = "gray") +
    geom_tile(data = output[which(output$x_bins>=0 & !is.na(output$x_bins)),], color = "black",fill=NA) +
    scale_fill_gradient2(limits=c(0, scale_max), low = "white", high= "purple",
                         midpoint =  scale_midpoint) +
    #scale_x_discrete(expand = c(0,0)) +
    scale_x_datetime(date_breaks = "6 months" , date_labels = "%m-%y",expand=c(0,0)) +
    ggtitle(paste(bin_label,"monthly",bt_str,"bin % presence")) +
    theme_bw() +
    theme(legend.title = element_text(size=12),
          legend.text = element_text(size=12)) +
    theme(axis.text = element_text(size=12),
          axis.title.y = element_blank()) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
    labs(fill = "% presence")


  return(base_map)

}


out = bin_label_explore<-function(conn,bin_label,inst_source='AFSC',plot_sds = 4,bin_type=1)


