# pyspark

Assorted files containing pySpark code and helper scripts

## scoringCandleLive.py

Scores a day's worth of news and blog articles

## scoreMonthOpeAndTwitter.ipynb

Original notebook for developing the scoring algorithm. Useful as a reference for Twitter scoring. Also contains various scratchcode and experimintation

## extractPubRanks.py

Script for scanning news/blog articles and extracting ranks

## launch_spark.sh
Shell script to launch EMR cluster

## emrpys
Shell script that launches an EMR cluster, runs pyspark in cluster mode (eg scoringCandleLive) and then shuts down the cluster

## sparkConfig.json

Spark configurations for better performance. emrpys and launch_spark.sh expect this file to be in S3

## installboto.sh

Script to install boto3 as a bootstrap step. Boto3 is used to distribute the reading of data from s3 as Spark's native s3 read functions are not distributed. emrpys and launch_spark.sh expect this file to be in S3
