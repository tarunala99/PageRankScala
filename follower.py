from pyspark import SparkConf, SparkContext

if __name__ == "__main__":

    conf = (SparkConf().setAppName("My app"))
    sc = SparkContext(conf = conf)
    file = sc.textFile("s3://cmucc-datasets/TwitterGraph.txt")
    counts = file.distinct().map(lambda word: (word.split("\t")[1], 1)).reduceByKey(lambda a,b: a + b)
    counts.saveAsTextFile("hdfs:///follower-output")
    sc.stop()
