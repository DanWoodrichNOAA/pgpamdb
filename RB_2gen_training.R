
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
bin_label = "RB" #can be single or vector of multiple
bin_type=3
exclude_procedure=c()
plot_type = "perc_effort" # perc_effort or perc_all_bins
include_children=TRUE
scale="None" #or log

resolution= "month" #date trunc valid values (https://www.codecademy.com/resources/docs/sql/dates/date-trunc)
date_breaks_ = "2 months" # ggplot date_breaks allowed https://www.rdocumentation.org/packages/scales/versions/1.2.1/topics/date_breaks
start_date = as.POSIXct("2014-01-01 00:00:00 UTC")
end_date = as.POSIXct("2016-01-01 00:00:00 UTC")
lat_min = 50
lat_max = 75
exclude_loc_codes = c()

bin_label_explore<-function(conn,bin_label,bin_type=1,plot_type="perc_effort",scale="None",
                            exclude_procedure=c(),include_children=TRUE,exclude_loc_codes=c(),
                            lat_min=-90,long_min=-180,lat_max=90,long_max=-140,start_date=as.POSIXct("2000-01-01 00:00:00",tz="UTC"),end_date=as.POSIXct(Sys.time()),
                            resolution="month",date_breaks_="6 months"){

  bt_str = c("LOW","REG","SHI","CUSTOM")[bin_type]

  query_ = "SELECT AVG(latitude) as avg_lat,AVG(longitude) as avg_long,location_code FROM data_collection GROUP BY location_code"

  avg_latlongs = dbFetch(dbSendQuery(conn,gsub("[\r\n]", "", query_)))

  avg_latlongs = avg_latlongs[which(!is.na(avg_latlongs$avg_lat)),]

  in_bounds_loc_codes = avg_latlongs[which(avg_latlongs$avg_long > long_min & avg_latlongs$avg_long < long_max
                                           & avg_latlongs$avg_lat > lat_min & avg_latlongs$avg_lat < lat_max),]

  if(length(exclude_loc_codes)>0){

    in_bounds_loc_codes = in_bounds_loc_codes[-which(in_bounds_loc_codes %in% exclude_loc_codes)]

  }


  if(length(exclude_procedure)>0){
    exclude_proc_string = paste("AND detections.procedure NOT IN (",paste(exclude_procedure,collapse=",",sep=""),")",sep="")
  }else{
    exclude_proc_string=""
  }

  bin_labels_sql = paste("('",paste(bin_label,collapse="','",sep=""),"')",sep="")

  if(include_children){
    bin_label_id = dbFetch(dbSendQuery(conn,gsub("[\r\n]", "", paste("SELECT id FROM signals WHERE code IN",bin_labels_sql))))
    bin_label_id = as.integer(bin_label_id$id)

    bin_label_query = paste("(with RECURSIVE
    n AS (
      SELECT * FROM signals WHERE parent_id IN (",bin_label_id,") OR id IN (",bin_label_id,")
      UNION ALL
      SELECT i.*
        from n
      JOIN signals i ON i.parent_id = n.id
    )
    SELECT DISTINCT code from n)")
  }else{
    bin_label_query = paste("(",paste(bin_label,collapse=",",sep=""),")",sep="")
  }

  query = paste("SELECT COUNT(CASE WHEN subquery.label IN (1,21) THEN 1 END) as p_bins, COUNT(CASE WHEN subquery.label IN (0,20) THEN 1 END) AS n_bins,subquery2.bin_total,
  subquery2.dt_,subquery2.location_code,subquery3.latitude FROM (SELECT DISTINCT ON (bins.id) detections.label,bins.id,COUNT(*),date_trunc('",resolution,"', soundfiles.datetime) AS dt_,data_collection.location_code,
  CASE detections.label WHEN 1 THEN 1 WHEN 21 THEN 2 WHEN 0 THEN 3 WHEN 20 THEN 4 ELSE 5 END AS label_preference
  FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON data_collection.id = soundfiles.data_collection_id
  JOIN bins_detections ON bins_detections.bins_id = bins.id JOIN detections ON bins_detections.detections_id = detections.id JOIN signals ON signals.id = detections.signal_code
  WHERE bins.type = ",bin_type," AND data_collection.institution_source = '",inst_source,"' AND data_collection.location_code IN ('",paste(unique(in_bounds_loc_codes$location_code),sep="",collapse="','"),"')
  AND signals.code IN ",bin_label_query," ",exclude_proc_string,"
  AND soundfiles.datetime >= '",start_date,"'::TIMESTAMP WITH TIME ZONE AND soundfiles.datetime <='",end_date,"'::TIMESTAMP WITH TIME ZONE
  GROUP BY detections.label,bins.id,dt_,data_collection.location_code
  ORDER BY bins.id,label_preference) AS subquery RIGHT JOIN (
                SELECT COUNT(*) AS bin_total,date_trunc('",resolution,"', soundfiles.datetime) AS dt_,
                data_collection.location_code
                FROM bins JOIN soundfiles ON bins.soundfiles_id = soundfiles.id JOIN data_collection ON
                data_collection.id = soundfiles.data_collection_id
                WHERE bins.type = ",bin_type," AND data_collection.institution_source = '",inst_source,"' AND data_collection.location_code IN ('",paste(unique(in_bounds_loc_codes$location_code),sep="",collapse="','"),"')
                AND soundfiles.datetime >= '",start_date,"'::TIMESTAMP WITH TIME ZONE AND soundfiles.datetime <='",end_date,"'::TIMESTAMP WITH TIME ZONE
                GROUP BY dt_,data_collection.location_code) AS subquery2
                ON subquery.dt_ = subquery2.dt_ AND subquery.location_code = subquery2.location_code
                JOIN (SELECT AVG(latitude) as latitude, location_code FROM data_collection GROUP BY location_code) AS subquery3
                ON subquery2.location_code = subquery3.location_code GROUP BY subquery2.bin_total,subquery2.dt_,subquery2.location_code,subquery3.latitude",sep="")

  output = dbFetch(dbSendQuery(conn,gsub("[\r\n]", "", query)))

  output$p_bins = as.integer(output$p_bins)
  output$n_bins = as.integer(output$n_bins)
  output$bin_total = as.integer(output$bin_total)

  if(plot_type=="perc_effort"){
    output$metric = output$p_bins/(output$p_bins+output$n_bins)
  }else if (plot_type=="perc_all_bins"){
    output$metric = output$p_bins/output$bin_total
    output$metric[which(output$n_bins==0 & output$p_bins==0)]=NA
  }else{
    stop("invalid plot type")
  }

  output$metric = output$metric*100

  if(scale=="log"){
    output$metric = log(output$metric+1)
    key = "log bin % presence:"
    key2 = "log % presence"
  }else{
    key = "bin % presence"
    key2 = "% presence"
  }

  colnames(output)[which(colnames(output)=="dt_")]="mm_yy"

  #order based on latitude
  output$location_code =factor(output$location_code,level = unique(output$location_code[order(output$latitude)]))

  base_map =ggplot(output, aes(mm_yy, location_code)) +
    geom_tile(aes(fill = metric), color = "gray") +
    geom_tile(data = output[which(output$metric>0 & !is.na(output$metric)),], color = "black",fill=NA) +
    scale_fill_gradient2(limits=c(0, max(output$metric,na.rm = TRUE)), low = "white", mid = "yellow",high= "red",
                         midpoint =  max(output$metric,na.rm = TRUE)/2) +
    #scale_x_discrete(expand = c(0,0)) +
    scale_x_datetime(date_breaks = date_breaks_ , date_labels = "%m-%y",expand=c(0,0)) +
    ggtitle(paste(bin_label,resolution,"timestep",bt_str,key)) +
    theme_bw() +
    theme(legend.title = element_text(size=12),
          legend.text = element_text(size=12)) +
    theme(axis.text = element_text(size=12),
          axis.title.y = element_blank()) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
    labs(fill = key2)


  return(base_map)

}


out = bin_label_explore(conn,"FW",inst_source='AFSC',plot_sds = 4,bin_type=1,plot_type="perc_effort")
test = add_layer_ble(con,6,10,"orange")

plot(base_map+test)

