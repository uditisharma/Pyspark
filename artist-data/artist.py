from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Count of artist")
sc= SparkContext(conf=conf)
lines=sc.textFile("/Users/dwaru/uditi-work/Pyspark/artist-data/unique_tracks.txt")
name=lines.map(lambda x: (x.split("<SEP>")[2],1))
count = name.reduceByKey(lambda x,y: x+y)
swapped = count.map()
results = count.take(10)
for result in results:
    print result
