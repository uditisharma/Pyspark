from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Count of artist")
sc= SparkContext(conf=conf)
lines=sc.textFile("/Users/dwaru/uditi-work/Pyspark/artist-data/unique_tracks.txt")
name=lines.map(lambda x: (x.split("<SEP>")[2],1))
count = name.reduceByKey(lambda x,y: x+y)
swapped = count.map(lambda (x,y):(y,x))
swapped_reversed = swapped.sortByKey(ascending=False)
out = swapped_reversed.map(lambda (x,y):(y,x))
results = out.take(10)
for result in results:
    print result[0],result[1]
