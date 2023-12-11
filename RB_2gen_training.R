
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
#let's take a look.

RBdata = bin_label_explore(con,"RB")

RBdata_og_vis = RBdata + add_layer_ble(con,23,10,"green")

#handlabeled fg- create a custom fg composed of detections. Register it as fg / custom bins.
#just query existing labels as training labels, take a look and quality control as needed.

#get the full ildiko dataset- positives, negatives, assumed negatives.
ildikoset_all = dbGet("SELECT * FROM detections WHERE procedure = 18")

#so when I did fin, I just pulled in the full bins and hand labeled what was missing. Any reason why
#this is different here?

#will still not get the missed calls back into training.
#fewer calls to box (even more of an argument to include)

#coverage is pretty good in the original training set.
#lets compare label quantities:
heloiseset_all= dbGet("SELECT * FROM detections WHERE procedure = 10 and signal_code = 23")

#the ildiko labels are definitely more numerous, but due to procedure simply just have to be looked over.
#can I do anything automated here? Doubtful. lots of other callers during ribbon time so simple threshold
#is no good.

#one thing to try: I could assess the marginal benefit of actually hand combing through the labels.
#divide the original set up by year categories.

#conditions to test, using metrics on test split as result:
#Use the rest of the original set
#Use the rest of the original set and a random portion of the new labels (unreviewed)
#Use the rest of the original set and the same portion of the new labels, reviewed.

#this could potentially make a good paper, very relevant to real world problems that are faced in model
#deployment. However, I think that a criticism would come up that since the 'test' dataset was used in
#the original training, it wouldn't be as succeptible to missed detections, since those detections
#would be less relevant/impactfull on the original dataset.

#so if I were to do this, I would probably want to use a new,
#sampled outside of detector dataset as the test dataset.

#how about attempting it for beardeds? That could be fine, but I will not have a previous detector
#to leverage.


#Idea for ribbons: I take the time to sample and labels a dataset of like, not more than 5000
#detections. let's for instance take a look at how much fin I labeled:

#34,000!
fin_num = dbGet("SELECT COUNT(*) FROM detections WHERE signal_code = 4 AND label = 1 AND procedure = 10")

#so, it would take me a fraction of the time, and if that were useful to potentially write a paper, then
#that's great. (such a paper could perhaps be the white literature for the new method as well)

#need to make sure I have a query ready to access the pre-modified dataset as needed.

#however this is totally forgetting- I am banking on some SC data coming in. And in that case,
#it will be entirely unboxed. If going down this road, I may want to include the SC data
#and include a routine to autobox it - maybe it is trash, maybe not. But idk if that will ever work
#with something like ribbon- I guess we could see??

#even if just for internal assessment, could be useful.

#so, what is there to do here if anything? Not sure, depends on whether I have to wait much longer for
#sc labels.

#tomorrow, will move towards bearded label formatting and go from there.


###just collect the currently labeled ribbon data to start developing stectrogram and model parameters:
fgs = dbGet("SELECT name FROM effort WHERE name LIKE '%rb%'")


