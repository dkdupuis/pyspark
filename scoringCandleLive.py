import pyspark

sc = pyspark.SparkContext()
sqlContext = pyspark.sql.SQLContext(sc)

import json
import operator
import datetime
from pyspark.sql import Row
import boto3
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *
import logging
from pyspark.sql.functions import lit
import sys
import pyspark

logging.basicConfig(filename='example.log',level=logging.WARN)

def scanOpeKeys(startKey, endKey):
    bucket = '***'
    startKey = startKey
    endKey = endKey
    bucket = boto3.resource('s3').Bucket(bucket)
    for obj in bucket.objects.filter(Marker=startKey):
        if obj.key < endKey: 
            yield obj.key
        else:
            return

def fluff(flat):
    deep = dict()
    for key in flat:
        d = deep
        path = key.split('/')
        while len(path) > 1:
            p = path.pop(0)
            d[p] = d.get(p, dict())
            d = d[p]
        d[path[0]] = flat[key]
    return deep

def readS3Ope(key):
    s3obj = boto3.resource('s3').Object(bucket_name='***', key=key)
    body = s3obj.get()['Body']
    art = body.read().decode('utf-8')
    body.close()
    return json.loads(art)

def bundleArticleData(fileLoc):
    try:
        entFilter = ['date', 'time', 'money', 'percentage', 'number', 'unknown', 'percent']
        flatDat = readS3Ope(fileLoc)
        art = fluff(flatDat)
        attrs = art.get('attrs')
        editorialRank = -10
        autoRank = -10
        for k in attrs:
            if 'feedRank' in attrs.get(k):
                autoRank = int(attrs.get(k)[9:])
            if 'source_rank' in attrs.get(k):
                editorialRank = int(attrs.get(k)[12:])
        publication = art.get('publication',('a'*38+'Unknown'))[38:]
        pubId = art.get('publication',('a'*38+'Unknown'))[1:37]
        mediaType = art.get('types',{}).get('0','Unknown')
        sentDict = art.get('sentences', {})
        sentences = []
        articleId = art.get('id')
        if pubId == 'a'*36 or publication == 'Unknown' or (mediaType != 'news' and mediaType != 'blog') or (editorialRank == -10 and autoRank == -10):
            return Row(failed=fileLoc, flag='missingLNInfo')
        for sentKey in sentDict:
            decay = .94 ** int(sentKey)
            sent = sentDict.get(sentKey)
            themesDict = sent.get('themes')
            entDict = sent.get('entities')
            sentenceText = sent['yield']
            mentions = []
            if not themesDict == None:
                mentions.extend([themesDict[k]['attributes']['TEXT'] for k in themesDict])
            if not entDict == None:
                for entKey in entDict:
                    if not entDict[entKey].get('types',{}).get('0', 'unknown') in entFilter:
                        mentions.extend([entDict[entKey].get('normalform')])
            mentions = [Row(theme=m, preMentionScore=decay) for m in mentions]
            if mentions:
                sentences.append(Row(sentenceId='{}.{:04d}'.format(articleId, int(sentKey)), sentenceText=sentenceText, mentions=mentions))
        if sentences:
            return Row(pubId=pubId, articleId=articleId, mediaType=mediaType, title=art.get('title', 'Unknown'), author=art.get('authorId', 'Unknown'),
                      publication=publication, editorialRank=editorialRank, autoRank=autoRank, sentences=sentences)
        else:
            return Row(failed=fileLoc, flag='noSentences')
    except:
        return Row(failed=fileLoc, flag='parseSomething')


def createTable(table, data, schema=None, samplingRatio=None):
    '''
    Creates a pyspark.sql data frame from 'data'.
    Registers it as a temp table named 'table'.
    Returns the data frame.
    '''
    t = sqlContext.createDataFrame(data, schema, samplingRatio)
    t.registerTempTable(table)
    return t

def mentionCount(article):
    score = 0
    for s in article['sentences']:
        for m in s['mentions']:
            score += m['preMentionScore']
    return score

def countMentionsByPub(articles):
    '''Creates a table of mention counts for each publication in articles.'''
    createTable('mentionCountsByPub', articles
                .map(lambda a: (a.publication, mentionCount(a)))
                .reduceByKey(lambda x,y: x + y),
                ['publication', 'preMentionScoreSum'])

def scoreSentenceMentions(sentence, mentionScoreMultiplier):
    '''Assigns mentionScore to each mention in sentence.'''
    s = sentence.asDict()
    s['mentions'] = [Row(theme=m['theme'], score=m['preMentionScore'] * mentionScoreMultiplier) for m in s['mentions']]
    return Row(**s)

def scoreArticleMentions(articleAndMentionScore):
    '''
    Assigns articleAndMentionScore.mentionScore as the score of each mention in article.
    Returns article (without mentionScore column).
    '''
    a = articleAndMentionScore.asDict()
    mentionScoreMultiplier = a.get('mentionScoreMultiplier', 0)
    a['sentences'] = [scoreSentenceMentions(s, mentionScoreMultiplier) for s in a['sentences']]
    del a['mentionScoreMultiplier']
    del a['dayAttention']
    return Row(**a)

def scoreArticles(articles):
    try:
        sqlContext.dropTempTable('articles')
    except:
        pass
    articles.registerTempTable('articles')
    countMentionsByPub(articles)
    scoredArticles = createTable(
        'scoredArticles',
        sqlContext.sql(
            'select a.*, 1.0 * a.dayAttention / p.preMentionScoreSum as mentionScoreMultiplier ' +
            'from articles a, mentionCountsByPub p where a.publication=p.publication'
        ).map(scoreArticleMentions))
    return scoredArticles

def article2QueriedSentences(article, queries):
    meta = article.asDict()
    del meta['sentences']
    for sent in article['sentences']:
        matchingTopics = set()
        sentText = sent['sentenceText'].lower()    
        for topic in queries:
            for term in queries[topic]:
                if term in sentText:
                    matchingTopics.add(topic)
        sentWithMeta = sent.asDict()
        sentWithMeta.update(meta)
        sentWithMeta['matchingTopics'] = list(matchingTopics)
        sentWithMeta['matchingTopics']
        yield Row(**sentWithMeta)

def getDayAttention(numArticlesToday=0, numArticlesWeek=0, editorialRank=0, autoRank=0):
    if autoRank > 0:
        minArts = 7 * {
            1:200, 2:40, 3:20, 4:15, 5:15, 6:15, 7:10, 8:7, 9:7, 10:7
        }.get(autoRank, 7)
        weekAttn = 7 * {
            1:1200, 2:200, 3:85, 4:50, 5:35, 6:25, 7:15, 8:10, 9:10, 10:10
        }.get(autoRank, 25)
    else:
        minArts = 7 * {
            1:290, 2:140, 3:50, 4:80, 5:100
        }.get(editorialRank, 100)
        weekAttn = 7 * {
            1:6750, 2:1575, 3:360, 4:250, 5:200
        }.get(editorialRank, 225)
    todayAttnShare = float(numArticlesToday)/max(numArticlesWeek,minArts)
    return todayAttnShare * weekAttn

getDayAttention_udf = UserDefinedFunction(getDayAttention, DoubleType())

def pullPubInfo(fileLoc):
    try:
        flatDat = readS3Ope(fileLoc)
        art = fluff(flatDat)
        attrs = art.get('attrs')
        editorialRank = -10
        autoRank = -10
        for k in attrs:
            if 'feedRank' in attrs.get(k):
                autoRank = int(attrs.get(k)[9:])
            if 'source_rank' in attrs.get(k):
                editorialRank = int(attrs.get(k)[12:])
        pubId = art.get('publication',('a'*38+'Unknown'))[1:37]
        mediaType = art.get('types',{}).get('0','Unknown')
        sentDict = art.get('sentences', {})
        sentences = True
        articleId = art.get('id')
        if pubId == 'a'*36 or (mediaType != 'news' and mediaType != 'blog') or (editorialRank == -10 and autoRank == -10):
            return Row(failed=fileLoc, flag='missingLNInfo')
        if sentences:
            return Row(pubId=pubId)
        else:
            return Row(failed=fileLoc, flag='noSentences')
    except:
        return Row(failed=fileLoc, flag='parseSomething')


def dayVolumeWritten(day):
    bucket = boto3.resource('s3').Bucket('***')
    key = 'pubArtsByDay/pubArts_' + day
    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) > 0 :
        return True
    else:
        return False

def dayScoreWritten(day):
    bucket = boto3.resource('s3').Bucket('***')
    key = 'scoredSentences/scoredSents_' + day
    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) > 0 :
        return True
    else:
        return False

def proccessDaysVolume(d):
    startKey = d
    endKey = (datetime.datetime.strptime(d, '%Y-%m-%d') + datetime.timedelta(1)).strftime('%Y-%m-%d')
    keys = list(scanOpeKeys(startKey, endKey))
    pkeys = sc.parallelize(keys,1000)
    parsed = pkeys.map(pullPubInfo)
    arts = sqlContext.createDataFrame(parsed.filter(lambda x: 'failed' not in x.asDict()))
    artCountByPubToday = arts.groupBy('pubId').count().withColumnRenamed('count','numArticlesDay')
    artCountByPubToday.write.parquet('s3://***/pubArtsByDay/pubArts_' + startKey)

def prepLeadingDays(scoringStartDate):
    firstDate = datetime.datetime.strptime(scoringStartDate, '%Y-%m-%d')
    prev6Days = [(firstDate-datetime.timedelta(i)).strftime('%Y-%m-%d') for i in range(1,7)]
    for d in prev6Days:
        if not dayVolumeWritten(d):
            logging.warn('Getting volume for day: ' + d)
            proccessDaysVolume(d)
        else:
            logging.warn('Day alaready has volume: ' + d)

def scoreDay(d):
    startKey = d
    endKey = (datetime.datetime.strptime(d, '%Y-%m-%d') + datetime.timedelta(1)).strftime('%Y-%m-%d')
    keys = list(scanOpeKeys(startKey, endKey))
    pkeys = sc.parallelize(keys,1000)
    parsed = pkeys.map(bundleArticleData)
    #parsed.cache()
    #parsed.count()
    arts = sqlContext.createDataFrame(parsed.filter(lambda x: 'failed' not in x.asDict()))
    arts.cache()
    artCountByPubToday = arts.groupBy('pubId').count().withColumnRenamed('count','numArticlesDay')
    artCountByPubToday.cache()
    prev6DaysPubArtsFileLoc = ['s3://***/pubArtsByDay/pubArts_' + 
                               (datetime.datetime.strptime(d, '%Y-%m-%d') - 
                                datetime.timedelta(i)).strftime('%Y-%m-%d') for i in range(1,7)]       
    artCoutByPubPrevDays = sqlContext.read.option("mergeSchema", "false").parquet(prev6DaysPubArtsFileLoc[0]).unionAll(
        sqlContext.read.option("mergeSchema", "false").parquet(prev6DaysPubArtsFileLoc[1])).unionAll(
            sqlContext.read.option("mergeSchema", "false").parquet(prev6DaysPubArtsFileLoc[2])).unionAll(
                sqlContext.read.option("mergeSchema", "false").parquet(prev6DaysPubArtsFileLoc[3])).unionAll(
                    sqlContext.read.option("mergeSchema", "false").parquet(prev6DaysPubArtsFileLoc[4])).unionAll(
                        sqlContext.read.option("mergeSchema", "false").parquet(prev6DaysPubArtsFileLoc[5]))
    #artCoutByPubPrevDays = sqlContext.read.option("mergeSchema", "false").parquet(prev6DaysPubArtsFileLoc)
    artCountByPubWeek = artCountByPubToday.unionAll(artCoutByPubPrevDays).groupBy(['pubId']).sum(
        'numArticlesDay').withColumnRenamed('sum(numArticlesDay)','numArticlesWeek')
    daysPub = artCountByPubToday.join(artCountByPubWeek,'pubId','left_outer').join(arts.select('pubId','editorialRank','autoRank').dropDuplicates(['pubId']), 'pubId', 'left_outer')
    dayPubsWithAttn = daysPub.withColumn('dayAttention', getDayAttention_udf(daysPub.numArticlesDay, daysPub.numArticlesWeek, daysPub.editorialRank, daysPub.autoRank))
    dayPubsWithAttn.cache()
    if not dayVolumeWritten(d):
        dayPubsWithAttn.select('pubId', 'numArticlesDay').write.parquet('s3://***/pubArtsByDay/pubArts_' + startKey)#datetime.datetime.today().strftime('%Y-%m-%d')
    artsWithAttention = arts.join(dayPubsWithAttn.select('pubId', 'dayAttention'), 'pubId', 'left_outer')
    artsWithAttention.cache()
    artsWithAttention.count()
    arts.unpersist()
    dayPubsWithAttn.unpersist()
    artsScored = scoreArticles(artsWithAttention)
    schema = StructType([StructField('articleId',StringType(),True),StructField('author',StringType(),True),StructField('autoRank',LongType(),True),StructField('editorialRank',LongType(),True),StructField('matchingTopics',ArrayType(StringType(),True),True),StructField('mediaType',StringType(),True),StructField('mentions',ArrayType(StructType([StructField('score',DoubleType(),True),StructField('theme',StringType(),True)]),True),True),StructField('pubId',StringType(),True),StructField('publication',StringType(),True),StructField('sentenceId',StringType(),True),StructField('sentenceText',StringType(),True),StructField('title',StringType(),True)])
    sqlContext.createDataFrame(artsScored.flatMap(lambda x: article2QueriedSentences(x, queries)),schema).write.parquet('s3://***/scoredSentences/scoredSents_' + startKey)                                     
    artsWithAttention.unpersist()

def scoreDays(startDate, endDate=None):
    if endDate == None:
        endDate = startDate
    dtStartDate = datetime.datetime.strptime(startDate, '%Y-%m-%d')
    dtEndDate = datetime.datetime.strptime(endDate, '%Y-%m-%d')
    t = dtStartDate
    daysToScore = []
    while t <= dtEndDate:
        d = t.strftime('%Y-%m-%d')
        if not dayScoreWritten(d):
            daysToScore.append(d)
        else:
            logging.warn('Day already scored: ' + d)
        t = t + datetime.timedelta(1)
    if daysToScore:
        prepLeadingDays(daysToScore[0])
    for d in daysToScore:
        logging.warn('Scoring day: ' + d)
        scoreDay(d)

cm = ['string 1', 'string 2']
um = ['string 1', 'string 2']
ce = ['string 1', 'string 2']
bd = ['string 1', 'string 2']
queries = {'contentMarketing':cm, 'urbanMobility':um, 'cleanEnergy':ce, 'bigData':bd}

endDate = None
startDate = sys.argv[1]
if len(sys.argv) > 2:
    endDate = sys.argv[2]
#sc = pyspark.SparkContext()
#sqlContext = pyspark.sql.SQLContext(sc)
scoreDays(startDate, endDate)
#print sc.parallelize(range(10)).map(lambda x: 2*x).collect()
