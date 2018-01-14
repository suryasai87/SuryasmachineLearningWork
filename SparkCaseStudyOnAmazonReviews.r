# Business understanding
# Based on the Amazon review dataset, the manager at a media company wants to choose a new product category.
# out of three options are movies and tv,cd and vinyl and kindle(ebooks).
# The three questions based on which the choice would depend are :
# Which product category has a larger market size
# Which product category is likely to be purchased heavily
# Which product category is likely to make the customers happy after the purchase
# As the sales data is not aviailable the proxt measures are used
# number of reviews is a proxy for number of product sold, number of reviewers is a proxy for market size and helpfulness
# index can help on the customer satisfaction.

# Setting up the paths to avoid any errors
.libPaths(c(.libPaths(), '/usr/lib/spark/R/lib'))
Sys.setenv(SPARK_HOME = '/usr/lib/spark') 

# Installing the sparklyr package locally. This step will take some time to install the library.
if ("sparklyr" %in% rownames(installed.packages()) == FALSE)
{install.packages("sparklyr")}
if ("ggplot2" %in% rownames(installed.packages()) == FALSE)
{install.packages("ggplot2")}
if ("DBI" %in% rownames(installed.packages()) == FALSE)
{install.packages("DBI")}
if ("dplyr" %in% rownames(installed.packages()) == FALSE)
{install.packages("dplyr")}


# Invoking the relevant libraries
library(sparklyr)
library(ggplot2)
library(DBI)
library(dplyr)

# Setting up a spark sessions
# Setting up the configuration, gives warnings, but no errors. !!!
conf <- spark_config()
conf$`sparklyr.shell.driver-memory` <- "6G"  # Use 6G only as 8 GB RAM is allocated for the cluster
conf$spark.memory.fraction <- 0.8 

# Checking the session info.
sessionInfo()

# Set up the spark context.
sc <- spark_connect(master = "local", config = conf)

# Lets load the Movies and TV data from s3 bucket.
df_MovieTV <- spark_read_json(sc,"table","s3://sparkanalysis/AmazonData/reviews_Movies_and_TV_5.json")


# Deduplication : seems no duplicaiton of data as number of rows before and after deduplication remains the same
nreviews_beforeDupe <-df_MovieTV %>% summarise(ncount = n()) %>% collect() # value
nreviews_beforeDupe

# removing dupes
df_MovieTV_rem <- df_MovieTV %>% group_by(reviewerID, asin) %>% filter(row_number(asin) == 1) %>% ungroup()
nreviews_afterDupe <-df_MovieTV_rem %>% summarise(ncount = n()) %>% collect() # value
nreviews_afterDupe

# Lets check the number of reviews which is number of entries which is an indication of number of 
# products sold, which answers second question of which products are likely to be purschase
# - 1697533
nreviews_mtv <-df_MovieTV_rem %>% summarise(ncount = n()) %>% collect() # value
nreviews_mtv

# Lets check the unique number of reviewers which is an indication of market size
# Number of unique reviewers - 123960
nreviewers_mtv <- df_MovieTV_rem %>%   summarise(ncount = n_distinct(reviewerID)) %>% collect()
nreviewers_mtv

# Lets check how is the distribution of reivew rating - more than 50% of the ratings are 5 
rev_dist_mtv <- df_MovieTV_rem %>% 
  select(overall) %>% 
  group_by(overall) %>%
  summarise(ratingcount = n()) %>%
  arrange(desc(ratingcount)) %>%
  collect() %>% 
  ggplot(aes(x = overall,y = ratingcount)) +
  geom_bar(stat="identity", fill="#e67e22", alpha=0.9) 
rev_dist_mtv

# From here onward we need just 3 columns overall rating,helpful score,text of review
df_MovieTV_reduced <- df_MovieTV_rem %>% select(helpful, overall, reviewText) %>% compute("tbl_red")

# List the data frames available in Spark
src_tbls(sc)

# Dropping tables not required any more : table which is original
DBI::dbGetQuery(sc, paste("DROP TABLE", "table"))

# Lets now look into the helpfulness score
# Unlisting the helpffulness score and bringing back to R environment for further processing
h_all <- df_MovieTV_reduced %>% select(helpful) %>% collect() %>% data.frame()
# Check for missing values - no missing values found
sum(is.na(h_all))
# Process the helpfulness score, to unpack the tuple
h_all %<>% 
  mutate(review_Nr = sapply(helpful,"[[", 1)) %>% 
  mutate(review_Dr = sapply(helpful,"[[", 2))  %>%
  mutate(reviewR = as.numeric(ifelse(review_Dr==0, 0, review_Nr/review_Dr))) %>%
  mutate(index = 1:n()) %>%
  select(index,review_Nr,review_Dr,reviewR)

# Copy this to Spark Environment
h_cpy <- copy_to(sc,h_all)
# Remove R dataframe
rm(h_all)

# Combining tables contained helpfulness parameters and overall score and reviewtext
combined_tbl <- sdf_bind_cols(h_cpy,df_MovieTV_reduced) %>% compute("combined")

# Drop the helpful column,as already unpacked and relevant fields extracted.
combined_tbl %<>% select(-helpful) %>% compute("combined")
# Spot check some elements
combined_tbl %>% head()

# List the data frames available in Spark
src_tbls(sc)

# Dropping tables not required any more : 
DBI::dbGetQuery(sc, paste("DROP TABLE", "h_all"))
DBI::dbGetQuery(sc, paste("DROP TABLE", "tbl_red"))

# Lets filter the rows which has less the 10 total helpful reviews
combined_tbl %<>% filter(review_Dr > 10) %>% compute("combined")
# Lets take a sample of data, for processing the word counts in the reviews
cb_tbl<- combined_tbl %>% 
  select(index,reviewText) %>% 
  sdf_sample(fraction = 0.001, replacement = FALSE, seed = 20000229) %>% 
  compute("tbl")

# Tokenise and explode to calculate the word count
df_nc <- cb_tbl %>% 
  ft_tokenizer("reviewText", "word") %>%
  mutate(nword = explode(word)) %>%
  group_by(index) %>%
  summarise(nc = n()) %>%
  select(nc) %>%
  compute("tbl_words")

# Works good on smaller data, now lets drop these intermediate tables
DBI::dbGetQuery(sc, paste("DROP TABLE", "tbl_words"))
DBI::dbGetQuery(sc, paste("DROP TABLE", "tbl"))

# Explore the original dataset, by finding the number of words in each review blob.
df_nc <- combined_tbl %>% 
  ft_tokenizer("reviewText", "word") %>%
  mutate(nword = explode(word)) %>%
  group_by(index) %>%
  summarise(nc = n()) %>%
  select(nc) %>%
  compute("tbl_words")

# Combine the word counts and helpfulness and overall scores in a dataframe 
final_tbl <- combined_tbl %>% select(review_Nr,review_Dr,reviewR,overall) %>% sdf_bind_cols(df_nc) %>% compute("final")

# Check the dataframe
final_tbl %>% head()

# Works good, now lets drop these intermediate tables
DBI::dbGetQuery(sc, paste("DROP TABLE", "tbl_words"))
DBI::dbGetQuery(sc, paste("DROP TABLE", "combined"))


# Lets visulaise the average helpfullness ratio over ratings : higher overall ratings of 4 & 5 have higher helpfulness score
rating_review <- final_tbl %>% group_by(overall) %>% 
  summarise(avg_helpfulnessscore = mean(reviewR)) %>%
  collect() %>%
  ggplot(aes(overall, avg_helpfulnessscore)) +
  geom_bar(stat="identity", fill="#e67e22", alpha=0.9) 
rating_review  

# Lets plot the histogram of the word length in the reviews : wordcounts are mostly in the range of 200-400,avg being 295.92
mean_nc <- final_tbl %>%
  select(nc) %>%
  summarise(avg_nc = mean(nc)) %>%
  collect()
mean_nc
nc_df <- final_tbl %>% select(nc) %>% 
  collect() %>%
  ggplot(aes(nc)) +
  geom_histogram(binwidth = 200) +
  geom_vline(xintercept=mean_nc %>% unlist(), linetype="dashed", color="red")
nc_df

# Lets bin the data in steps of 200 and plot the average helpfulness score in each bin
wordcounts = as.numeric(c(1,201,401,601,801,1001,1201,1401,1601,1801,Inf))
wc_labels =c("0-200","200-400","400-600","600-800","800-1000","1000-1200","1200-1400","1400-1600","1600-1800","1800-Inf")

nc_helpfulness <- final_tbl %>%
  # Select review text and number of words count
  select(reviewR, nc) %>%
  # Convert number of words to numeric
  mutate(nc = as.numeric(nc)) %>%
  # Bucketize word count to buckets using wordcounts vector
  ft_bucketizer("nc", "nc_bucket",splits = wordcounts) %>%
  # Collect the result
  collect() %>%
  # Convert nc_bucket to factor using wc_labels
  mutate(nc_bucket = factor(nc_bucket,labels = wc_labels)) 

# Visualisation to show the variation of helpfulness index with respect to word counts buckets
g1 <- nc_helpfulness %>%
  # grouping by the nc_bucket
  group_by(nc_bucket) %>%
  summarise(avg_hlp = mean(reviewR)) %>%
  ggplot(aes(nc_bucket,avg_hlp,group=1)) +
  geom_line(color = "red")

g1

# Based on the trend line it is clear that the length of the review
# has any no bearing on the helpfulness score, surprisingly the helpfulness
# score increases after 1000 words and then drops at 1400 words and then again increases.
# so taking the overall ranking of 4 & 5 as the indicator of helpfulness and subsequently the
# custormer satisfaction, we will count the number of reviews with overall 4 & 5 ratings and find perccentage of total reviews.
star5count <- final_tbl %>% 
  select(overall) %>%
  filter (overall > 3) %>%
  summarise(star5count = n()) %>%
  collect()
star5count
# Overall ratio of 4 & 5 ratings is :
# Percentage of total 104714/191616 ~ 54.65% 
# We can consider this number as a customer satisfaction indicator

# drop the table in spark
DBI::dbGetQuery(sc, paste("DROP TABLE", "final"))
# remove the variables and pointers from R environment except the spark context
rm(list=setdiff(ls(), "sc"))

# Dataset 2 : Lets explore the CD & Vinyl dataset, load the data in spark dataframe from S3 bucket
df_cd <- spark_read_json(sc,"cdtmptable","s3://sparkanalysis/AmazonData/reviews_CDs_and_Vinyl_5.json")

# Deduplication : seems no duplicaiton of data
cdreviews_beforeDupe <-df_cd %>% summarise(ncount = n()) %>% collect() # value
cdreviews_beforeDupe
# The above output was 1097592 records


# removing dupes
df_cdStore_rem <- df_cd %>% group_by(reviewerID, asin) %>% filter(row_number(asin) == 1) %>% ungroup()
ncdreviews_afterDupe <-df_cdStore_rem %>% summarise(ncount = n()) %>% collect() # value
ncdreviews_afterDupe
# The above output was also 1097592 records. Hence there are no duplicate records.


# Lets check the number of reviews which is number of entries which is an indication of number of 
# products sold, which answers second question of which products are likely to be purchased
# - 1097592
nreviews_cdstore <-df_cdStore_rem %>% summarise(ncount = n()) %>% collect() # value
nreviews_cdstore


# Lets check the unique number of reviewers which is an indication of market size
# Number of unique reviewers - 75258
nreviewers_cdstore <- df_cdStore_rem %>%   summarise(ncount = n_distinct(reviewerID)) %>% collect()
nreviewers_cdstore


# Lets check how is the distribution of reivew rating - more than 50% of the ratings are 5 
rev_dist_cd <- df_cdStore_rem %>% 
  select(overall) %>% 
  group_by(overall) %>%
  summarise(ratingcount = n()) %>%
  arrange(desc(ratingcount)) %>%
  collect() %>% 
  ggplot(aes(x = overall,y = ratingcount)) +
  geom_bar(stat="identity", fill="#e67e22", alpha=0.9) 

rev_dist_cd


# From here onward we need just 3 columns overall rating,helpful score,text of review
df_cdStore_Threecols <- df_cdStore_rem %>% select(helpful, overall, reviewText) %>% compute("tbl_cdStoreThreecols")

# List the data frames available in Spark
src_tbls(sc)

# Dropping tables not required any more : "cdtmptable" which wasbuilt in the intial stages
DBI::dbGetQuery(sc, paste("DROP TABLE", "cdtmptable"))

# Lets now look into the helpfulness score
# Unlisting the helpffulness score and bringing back to R environment for further processing
helpfullnessscore_cd_all <- df_cdStore_Threecols %>% select(helpful) %>% collect() %>% data.frame()

nrow(helpfullnessscore_cd_all)
#1097592 observations for helpfullnessscore_cd_all

# Check for missing values - no missing values found
sum(is.na(helpfullnessscore_cd_all))
# Process the helpfulness score to extract the number numbers in tuple as well their ratios, taking care of 
# divide by zero error as well.
helpfullnessscore_cd_all %<>% 
  mutate(cdreview_Nr = sapply(helpful,"[[", 1)) %>% 
  mutate(cdreview_Dr = sapply(helpful,"[[", 2))  %>%
  mutate(cdreviewR = as.numeric(ifelse(cdreview_Dr==0, 0, cdreview_Nr/cdreview_Dr))) %>%
  mutate(index = 1:n()) %>%
  select(index,cdreview_Nr,cdreview_Dr,cdreviewR)


# Copy this df to Spark Environment
helpfullnessscore_cd_cpy <- copy_to(sc,helpfullnessscore_cd_all)
# Remove R dataframe
rm(helpfullnessscore_cd_all)


# Combining tables containing the helpfulness elements, overall score and review text blob.
combined_cd_tbl <- sdf_bind_cols(helpfullnessscore_cd_cpy,df_cdStore_Threecols) %>% compute("combined")

# Drop the helpful column,as we have extracted the tuple already. 
combined_cd_tbl %<>% select(-helpful) %>% compute("combined")
combined_cd_tbl %>% head()


# List the data frames available in Spark
src_tbls(sc)


# Dropping tables not required any more : 
DBI::dbGetQuery(sc, paste("DROP TABLE", "helpfullnessscore_cd_all"))
DBI::dbGetQuery(sc, paste("DROP TABLE", "tbl_cdstorethreecols"))


# Lets filter the rows which has less the 10 total helpful reviews
combined_cd_tbl %<>% filter(cdreview_Dr > 10) %>% compute("combined")



# Explore the dataset, to calculate the number of words in the review writeup
df_cd_reviewtest_wordcount_nc <- combined_cd_tbl %>% 
  ft_tokenizer("reviewText", "cdword") %>%
  mutate(nword = explode(cdword)) %>%
  group_by(index) %>%
  summarise(numchars = n()) %>%
  select(numchars) %>%
  compute("temp_tbl_cd_words")



# Combine the word counts and helpfulness and overall scores and word counts in a dataframe 
final_cd_tbl <- combined_cd_tbl %>% select(cdreview_Nr,cdreview_Dr,cdreviewR,overall) %>% sdf_bind_cols(df_cd_reviewtest_wordcount_nc) %>% compute("final")

# Check the dataframe
final_cd_tbl %>% head()

# Works good, now lets drop these intermediate tables
DBI::dbGetQuery(sc, paste("DROP TABLE", "temp_tbl_cd_words"))
DBI::dbGetQuery(sc, paste("DROP TABLE", "combined"))


# Lets check the average helpfullness ratio over ratings : higher overall ratings in 4 & 5 
rating_cd_review <- final_cd_tbl %>% group_by(overall) %>% 
  summarise(avg_rating = mean(cdreviewR)) %>%
  collect() %>%
  ggplot(aes(overall, avg_rating)) +
  geom_bar(stat="identity", fill="#e67e22", alpha=0.9) 

rating_cd_review  


# Lets plot the histogram of the word length in the reviews : wordcounts are mostly in the range of 200-400,avg being 270.74
mean_nc <- final_cd_tbl %>%
  select(numchars) %>%
  summarise(avg_nc = mean(numchars)) %>%
  collect()
mean_nc
# Lets visualise this.
nc_df <- final_cd_tbl %>% select(numchars) %>% 
  collect() %>%
  ggplot(aes(numchars)) +
  geom_histogram(binwidth = 200) +
  geom_vline(xintercept=mean_nc %>% unlist(), linetype="dashed", color="red")
nc_df

# Lets bin the data and plot the average helpfulness score in each bin
wordcounts = as.numeric(c(1,201,401,601,801,1001,1201,1401,1601,1801,Inf))
wc_labels =c("0-200","200-400","400-600","600-800","800-1000","1000-1200","1200-1400","1400-1600","1600-1800","1800-Inf")

nc_helpfulness <- final_cd_tbl %>%
  # Select review text and number of words count
  select(cdreviewR, numchars) %>%
  # Convert number of words to numeric
  mutate(numchars = as.numeric(numchars)) %>%
  # Bucketize word count to buckets using wordcounts vector
  ft_bucketizer("numchars", "nc_bucket",splits = wordcounts) %>%
  # Collect the result
  collect() %>%
  # Convert nc_bucket to factor using wc_labels
  mutate(nc_bucket = factor(nc_bucket,labels = wc_labels)) 

# Visualiste the average helpfullness ratio in each of the word counts buckets.
g1 <- nc_helpfulness %>%
  # grouping by the nc_bucket
  group_by(nc_bucket) %>%
  summarise(avg_hlp = mean(cdreviewR)) %>%
  ggplot(aes(nc_bucket,avg_hlp,group=1)) +
  geom_line(color = "red")

g1

# Based on the trend line it is clear that the length of the review
# has any no bearing on the helpfulness score, surprisingly the healpfulness
# score increases till 1000 words and then drops at 1800 words and then again increases.
# so taking the overall ranking of 4 & 5 as the indicator of helpfulness and subsequently the
# custormer satisfaction, we will count the number of reviews with overall 4 & 5 ratings and then find overall ratio
star5count <- final_cd_tbl %>% 
  select(overall) %>%
  filter (overall > 3) %>%
  summarise(star5count = n()) %>%
  collect()
star5count
# Overall ratio of 4 & 5 ratings is :
# 92227/146324 ~ 63.29 % 
# We can consider this number as a customer satisfaction indicator

# drop the table in spark
DBI::dbGetQuery(sc, paste("DROP TABLE", "final"))
# remove the variables and pointers from R environment except the sc
rm(list=setdiff(ls(), "sc"))

# Finally, lets explore the Kindle dataset on S3
df_KindleStore <- spark_read_json(sc,"bigfourkindletmptable","s3://sparkanalysis/AmazonData/reviews_Kindle_Store_5.json")


# Deduplication : seems no duplicaiton of data
nkindlereviews_beforeDupe <-df_KindleStore %>% summarise(ncount = n()) %>% collect() # value
nkindlereviews_beforeDupe
# The above output was 982619 records


# removing dupes
df_KindleStore_rem <- df_KindleStore %>% group_by(reviewerID, asin) %>% filter(row_number(asin) == 1) %>% ungroup()
nkindlereviews_afterDupe <-df_KindleStore_rem %>% summarise(ncount = n()) %>% collect() # value
nkindlereviews_afterDupe
# The above output was also 982619 records. Hence there are no duplicate records.


# Lets check the number of reviews which is number of entries which is an indication of number of 
# products sold, which answers second question of which products are likely to be purchased
# - 982619
nreviews_kindlestore <-df_KindleStore_rem %>% summarise(ncount = n()) %>% collect() # value
nreviews_kindlestore

# Lets check the unique number of reviewers which is an indication of market size
# Number of unique reviewers - 68223
nreviewers_kindlestore <- df_KindleStore_rem %>%   summarise(ncount = n_distinct(reviewerID)) %>% collect()
nreviewers_kindlestore

# Lets check how is the distribution of reivew rating - more than 50% of the ratings are 5 
rev_dist_ks <- df_KindleStore_rem %>% 
  select(overall) %>% 
  group_by(overall) %>%
  summarise(ratingcount = n()) %>%
  arrange(desc(ratingcount)) %>%
  collect() %>% 
  ggplot(aes(x = overall,y = ratingcount)) +
  geom_bar(stat="identity", fill="#e67e22", alpha=0.9) 

rev_dist_ks

# From here onward we need just 3 columns overall rating,helpful score,text of review
df_KindleStore_Threecols <- df_KindleStore_rem %>% select(helpful, overall, reviewText) %>% compute("tbl_KindleStoreThreecols")

# List the data frames available in Spark
src_tbls(sc)

# Dropping tables which is not required any more : "bigfourkindletmptable" which wasbuilt in the intial stages
DBI::dbGetQuery(sc, paste("DROP TABLE", "bigfourkindletmptable"))

# Lets now look into the helpfulness score
# Unlisting the helpffulness score and bringing back to R environment for further processing
helpfullnessscore_ks_all <- df_KindleStore_Threecols %>% select(helpful) %>% collect() %>% data.frame()

#982619 observations for helpfullnessscore_ks_all

# Check for missing values - no missing values found
sum(is.na(helpfullnessscore_ks_all))
# Process the helpfulness score to extract the number numbers in tuple as well their ratios, taking care of 
# divide by zero error as well.
helpfullnessscore_ks_all %<>% 
  mutate(ksreview_Nr = sapply(helpful,"[[", 1)) %>% 
  mutate(ksreview_Dr = sapply(helpful,"[[", 2))  %>%
  mutate(ksreviewR = as.numeric(ifelse(ksreview_Dr==0, 0, ksreview_Nr/ksreview_Dr))) %>%
  mutate(index = 1:n()) %>%
  select(index,ksreview_Nr,ksreview_Dr,ksreviewR)

# Copy this df to Spark Environment
helpfullnessscore_ks_cpy <- copy_to(sc,helpfullnessscore_ks_all)
# Remove R dataframe
rm(helpfullnessscore_ks_all)

# Combining tables to get helpfulness paramters, overall score and review blob.
combined_ks_tbl <- sdf_bind_cols(helpfullnessscore_ks_cpy,df_KindleStore_Threecols) %>% compute("combined")

# Drop the helpful column,as we have extracted the tuple already. 
combined_ks_tbl %<>% select(-helpful) %>% compute("combined")
# Spot check.
combined_ks_tbl %>% head()

# List the data frames available in Spark
src_tbls(sc)

# Dropping tables not required any more : 
DBI::dbGetQuery(sc, paste("DROP TABLE", "helpfullnessscore_ks_all"))
DBI::dbGetQuery(sc, paste("DROP TABLE", "tbl_KindleStoreThreecols"))

# Lets filter the rows which has less the 10 total helpful reviews
combined_ks_tbl %<>% filter(ksreview_Dr > 10) %>% compute("combined")

# Explore the original dataset, count the number of words in the review blobs.
df_ks_reviewtest_wordcount_nc <- combined_ks_tbl %>% 
  ft_tokenizer("reviewText", "ksword") %>%
  mutate(nword = explode(ksword)) %>%
  group_by(index) %>%
  summarise(numchars = n()) %>%
  select(numchars) %>%
  compute("temp_tbl_ks_words")

# Combine the word counts and helpfulness and overall scores in a dataframe 
final_ks_tbl <- combined_ks_tbl %>% select(ksreview_Nr,ksreview_Dr,ksreviewR,overall) %>% sdf_bind_cols(df_ks_reviewtest_wordcount_nc) %>% compute("final")

# Check the dataframe
final_ks_tbl %>% head()

# Works good, now lets drop these intermediate tables
DBI::dbGetQuery(sc, paste("DROP TABLE", "temp_tbl_ks_words"))
DBI::dbGetQuery(sc, paste("DROP TABLE", "combined"))


# Lets check the average helpfullness ratio over ratings : higher overall ratings in 4 & 5  
rating_ks_review <- final_ks_tbl %>% group_by(overall) %>% 
  summarise(avg_rating = mean(ksreviewR)) %>%
  collect() %>%
  ggplot(aes(overall, avg_rating)) +
  geom_bar(stat="identity", fill="#e67e22", alpha=0.9) 

rating_ks_review  
# Lets plot the histogram of the word length in the reviews : wordcounts are mostly in the range of 200-400,avg being 206.6
mean_nc <- final_ks_tbl %>%
  select(numchars) %>%
  summarise(avg_nc = mean(numchars)) %>%
  collect()
mean_nc
# Visualise thus.
nc_df <- final_ks_tbl %>% select(numchars) %>% 
  collect() %>%
  ggplot(aes(numchars)) +
  geom_histogram(binwidth = 200) +
  geom_vline(xintercept=mean_nc %>% unlist(), linetype="dashed", color="red")
nc_df

# Lets bin the data and plot the average helpfulness score in each bin
wordcounts = as.numeric(c(1,201,401,601,801,1001,1201,1401,1601,1801,Inf))
wc_labels =c("0-200","200-400","400-600","600-800","800-1000","1000-1200","1200-1400","1400-1600","1600-1800","1800-Inf")

nc_helpfulness <- final_ks_tbl %>%
  # Select review text and number of words count
  select(ksreviewR, numchars) %>%
  # Convert number of words to numeric
  mutate(numchars = as.numeric(numchars)) %>%
  # Bucketize word count to buckets using wordcounts vector
  ft_bucketizer("numchars", "nc_bucket",splits = wordcounts) %>%
  # Collect the result
  collect() %>%
  # Convert nc_bucket to factor using wc_labels
  mutate(nc_bucket = factor(nc_bucket,labels = wc_labels)) 

g1 <- nc_helpfulness %>%
  # grouping by the nc_bucket
  group_by(nc_bucket) %>%
  summarise(avg_hlp = mean(ksreviewR)) %>%
  ggplot(aes(nc_bucket,avg_hlp,group=1)) +
  geom_line(color = "red")
# Visualisation of avg helpfulness v/s the words bins.
g1

# Based on the trend line it is clear that the length of the review
# has a bearing till almost 1200 words and then if the length of 
# review is hihger then helpful ness score decrease. Lets in this case again 
# stick to same proces of using only 4 & 5 star ratings a proxy of customer satisfaction
star5count <- final_ks_tbl %>% 
  select(overall) %>%
  filter (overall > 3) %>%
  summarise(star5count = n()) %>%
  collect()
star5count

# Overall ratio of 4 & 5 ratings is: 
# 11778/18218 ~ 64.65 % 
# We can consider this number as a customer satisfaction indicator

# drop the table in spark
DBI::dbGetQuery(sc, paste("DROP TABLE", "final"))
# remove the variables and pointers from R environment except the sc
rm(list=setdiff(ls(), "sc"))

# Disconnect the spark session
spark_disconnect(sc)

# Remove the sc as well. 
rm(list = ls())
# Final thoughts on chooosing the product category
# Lets now consilidate the results.

# For Movies and Tv Dataset
# 1697533, number of reviews which is a proxy products likely to be purchased
# 123960, number of unique reviewers, which is a proxy for the market size
# 54.65 % ,Customer satisfacton, evlauted based on ratio of total number of overall 4 & 5 scores to total number of overall scores
# also found that the lenght of reviews has a huge variation doesnt follow a upward or
# downward trend
# 295, average words counts in a review 

# For CD & Vinyl  Dataset
# 1097592, number of reviews which is a proxy products likely to be purchased
# 75258, number of unique reviewers, which is a proxy for the market size
# 63.03 % ,Customer satisfacton, evlauted based on ratio of total number of overall 4 & 5 scores to total number of overall scores
# also found that the lenght of reviews has a huge variation doesnt follow a upward or
# downward trend
# 270, average words counts in a review 

# For Kindle dataset  Dataset
# 982619, number of reviews which is a proxy products likely to be purchased
# 68223, number of unique reviewers, which is a proxy for the market size
# 64.65 % ,Customer satisfacton, evlauted based on ratio of total number of overall 4 & 5 scores to total number of overall scores
# also found that the length of reviews follow an upward trend till thousand words and then plummnet, so using overall scores
# ratio to decide the customer satisfaction
# 206, average words counts in a review 

# The Final Suggestion for the Business.
# Based on the above numbers, the Movies and TV product category clearly wins in terms of 
# product likely to be purchased and market size which are indicators of presales information, however the 
# customer satisfaction which on the lower side as compared to the CD & Vinyl and Kindle product category
# Also, the average number of words written in the review is highest in this dataset, which means to say people
# write longer reviews for this product category.
# Further,using these statistics and considering that the key performance metric for business should be higher sales and given the fact
# customer satisfaction is almost very close for other two categories and not really off by huge margin 55% versus 65%.
# the decision of the business should be to be launch the product category of Movies and TV products.




