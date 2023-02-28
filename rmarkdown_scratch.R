#rmarkdown scratch

## Loading the data sources into the database

For use in training, as well as for general use and recordkeeping purposes, we can load these training sample back into the database as entries in 'effort'. This has already been completed prior to the creation of this Rmarkdown, but here is the code which originally loaded in the data. Let's look at the first row of the effort table:

```{r}
print(dbGet("SELECT * FROM effort LIMIT 1"))
```

Effort lets us id, name and describe each sample.

```{r}

effort_table = data.frame(c("lm2gen_og_train","lm2gen_train_pos_set_no_ovlp","lm2gen_train_rand_set_no_ovlp","lm2gen_hardneg_ds","lm2gen_oddtp"),c("semi-random no overlap","high grade random no overlap","random no overlap","hard negative downsampled","procedural"),
                          c("The ground truth training used in the previous LM detector: BS13_AU_PM02-a_files_343-408_lm, BS14_AU_PM04_files_189-285_lm, BS14_AU_PM04_files_304-430_lm, BS14_AU_PM04_files_45-188_lm, BS13_AU_PM02-a_files_38-122_lm, BS13_AU_PM02-a_files_510-628_lm",
                                                                                                                                                                                                                                                                              "LMyesSample_1 without overlap from lm2gen_og_train (1st gen training set)","oneHRonePerc without overlap from lm2gen_og_train (1st gen training set) OR lm2gen_train_pos_set_no_ovlp",
                                                                                                                                                                                                                                                                              "Hard negatives from LM 1st gen detector deployment. >=.95 probability, and downsampled by factor of 9. No overlap with lm2gen_og_train, lm2gen_train_pos_set_no_ovlp, lm2gen_train_rand_set_no_ovlp",

            "True positives from less common sites (minus all with > 100 LM tp/mooring deployment avg: BS03 PM02 PM04) from LM 1st gen deployment. no overlap with lm2gen_og_train, lm2gen_train_pos_set_no_ovlp, lm2gen_train_rand_set_no_ovlp, or lm2gen_hardneg_ds"))

colnames(effort_table) = c('name','sampling_method','description')

#dbAppendTable(con,'effort',effort_table) #commented out since it's already been run

```

We also need to enter info to allow us to relate effort back to the component bins ("bins_effort") and track assumptions between the segment of effort and the procedure ("effort_procedures"). Let's look at the first row of the effort_procedures table.

```{r}
print(dbGet("SELECT * FROM effort_procedures LIMIT 1"))
```

This table allows us to relate the status of a unit of effort for a specific procedure, along with the relevant signal code. Other info in this relation is the type of relationship, for instance, we are using this data for 'train_eval' so our current samples would also fit that category. This could also describe a data sample which was used for only detector inference. Assumption refers to the status of 'empty space' produced in the analysis- an 'i_neg' (implied negative) assumption means that empty space between detections is assumed to be a human verified no (0). This could also be 'bin_negative_LOW', such as the low moan deployment, which assumes that bins in which there were no verified low moans are a protocol no (20). Completed is a way of tracking whether this assumption applies - for instance, if a implied negative training sample were currently being annotated, it would be innapropriate to assume that the empty space indicates no positive signal presence.

Since this table has a foreign key on the effort table, the first step will be to relate the automatically assigned effort ids back to our data samples, and then construct the effort_procedures table for upload.

```{r}

#retrieve the new effort ids:

effort_table_wid = dbGet("SELECT * FROM effort WHERE name IN ('lm2gen_og_train','lm2gen_train_pos_set_no_ovlp','lm2gen_train_rand_set_no_ovlp','lm2gen_hardneg_ds','lm2gen_oddtp')")

effort_procedures = effort_table_wid[,c(1,2)]

effort_procedures = effort_procedures[order(effort_procedures$id),]

#the procedures for the  miscellaneous training data, such as for this and the previous retraining, are id 10. However, the procedure for the positive sample (13) and negative sample (12) have specific procedures as independent studies. When the INSTINCT training process queries GT, it will simply take a union of signals from the procedures 10,12, and 13, and is instructed to remove overlapping boxes for purposes of training.

effort_procedures = data.frame(effort_procedures$id,c(10,13,12,10,10),3,"train_eval","i_neg",c("y","y","y","n","n"),effort_procedures$name)

colnames(effort_procedures) = c("effort_id","procedures_id","signal_code","effproc_type","effproc_assumption","completed")

#dbAppendTable(con,'effort_procedures',effort_procedures) #submit the data to the database

```

The last step is to link the effort to bins (sample below)

```{r}
print(dbGet("SELECT * FROM bins WHERE type = 1 LIMIT 1"))
```

With the join table bins_effort

```{r}
print(dbGet("SELECT * FROM bins_effort LIMIT 1"))
```

We will use the vectors of bins we pulled from the database and removed duplicates from earlier in the doc.

```{r}

#refer to the effort table created earlier to link names to ids

allbins = list()

allbins[[1]] = data.frame(lm2gen_og_train,248)
colnames(allbins[[1]]) = c("bins_id","effort_id")

allbins[[2]] = data.frame(lm2gen_train_pos_set_no_ovlp,249)
colnames(allbins[[2]]) = c("bins_id","effort_id")

allbins[[3]] = data.frame(lm2gen_train_rand_set_no_ovlp,250)
colnames(allbins[[3]]) = c("bins_id","effort_id")

allbins[[4]] = data.frame(lm2gen_hardneg_ds,251)
colnames(allbins[[4]]) = c("bins_id","effort_id")

allbins[[5]] = data.frame(lm2gen_oddtp,252)
colnames(allbins[[5]]) = c("bins_id","effort_id")

allbins = do.call("rbind",allbins)

#dbAppendTable(con,'bins_effort',allbins) #submit to db

```

The data are now fully loaded!
