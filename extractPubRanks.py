#Requires boto3 installed on all nodes of cluster

import boto3
import json
import numbers
import uuid

def extractPubRankStuffs(keyToRead):
    #Create null class for UUID
    class NULL_NAMESPACE:
        bytes = b''       
    #Pull master data set file    
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name = '***', key = keyToRead)
    resp = obj.get()   
    #read response, decode it, parse the json
    respBody = json.loads(resp['Body'].read().decode("utf-8"))   
    #Create empty list for article meta data    
    art = []  
    #loop through the articles from master dataset file
    for articles in respBody.get('articles',list()):
        #many "get" calls to traverse the dictionary based on a article
        source = articles.get('source',dict())
        feed = source.get("feed",dict())
        medTyp = feed.get("mediaType","")      
        #Only interested in ranks of News and Blogs
        if medTyp == 'News' or medTyp == 'Blog':
            lang = articles.get("language","")
            medTyp = feed.get("mediaType","")
            edRank = int(source.get("editorialRank",-100))
            autRank = int(feed.get("rank",dict()).get("autoRank",-100))
            pubName = source.get("name","")      
            if medTyp == 'Blog':               
                #generating key for uuid
                pubKey = '***/publication-' + pubName              
                #Ranking for blogs is 10 + autRank (range 11-20);
                rankEx = autRank + 10
            else:              
                #generating key for uuid
                rankEx = edRank
                #Ranking for News is editorialRank (range is 1-5)
                pubKey = '***/publication-' + pubName     
            #get UUID key for publisher
            pubUUID = str(uuid.uuid3(NULL_NAMESPACE,pubKey.encode('utf-8')))     
            art.append(pyspark.sql.Row(**dict([('name',pubName), ('pubUUID',pubUUID), ('pubRank',rankEx), ('language',lang), ('mediaType', medTyp), ('editorialRank', edRank), ('autoRank', autRank)])))
    return art
    
    
#Get keylist for files
testS3 = boto3.client('s3').get_paginator('list_objects').paginate(Bucket = '***', Prefix = 'moreover/2015/12/10/05/09')

keyList = list()
for pages in testS3:
    for obj in pages.get('Contents'):
            keyList.append(obj.get('Key'))

#Prepare keys for mapping to executors
pkeys = sc.parallelize(keyList,4000)    

#Distribute read, extraction, & pubRank calculation to executors
pubMap = pkeys.flatMap(extractPubRankStuffs)

#Drop duplicate entries for publishers with more than 1 article
distPub = pubMap.distinct(1000).toDF()

#Filter illegal values
legalVals = distPub.filter("(mediaType = 'News' AND (pubRank >= 1 AND pubRank <= 5)) OR (mediaType = 'Blog' AND (pubRank >= 11 AND pubRank <= 20))").cache()

#Method for dealing with publications with multiple rankings
#Find min rank (aka most influential rank)
legalDups = legalVals.groupBy('name').min('pubRank').join(legalVals, "name", "left_outer").withColumnRenamed('min(pubRank)','minPubRank')
#Keep only the minimum rank
finalPubranks = legalDups.filter('minPubRank = pubRank')
#Write results to s3
finalPubranks.write.format('com.databricks.spark.csv').save('s3://***/pubRanksWriteTest')