import org.apache.spark.sql.SparkSession
object PageRank{
def main(args: Array[String]): Unit = {
val spark = SparkSession.builder().appName("Broadcast Test").getOrCreate()

val sc = spark.sparkContext
var file = sc.textFile("s3://cmucc-datasets/TwitterGraph.txt")
val links = file.distinct().map(word => (word.split("\t")(0),word.split("\t")(1))).groupByKey()
var ranks1=file.flatMap(line => line.split("\t")).map(word => (word,1.00)).distinct()
var ranks3=file.flatMap(line => line.split("\t")).map(word => (word,1.00)).distinct()

val total=2315848.00
var a = 0;
for(a <- 1 to 10)
{
	val can = ranks3.map({case (x,y) => (x,y)}).subtractByKey(links.map({case (x,y) => (x,1)}))
	val dangle = can.map({case (x,y) => y}).reduce(_+_)
	val contd = ranks3.map({case (x,y) => (x,dangle/total)})
	val contribs = links.join(ranks1).flatMap({case (url, (links, rank)) =>links.map(dest => (dest, rank/links.size))})
	val contribs1 = contribs.union(contd)
	ranks1 = contribs1.reduceByKey((x,y) => x+y).mapValues(sum => 0.15+0.85*sum)
}	
val ranks2=ranks1.map(func1)
ranks2.saveAsTextFile("hdfs:///pagerank-output")
spark.stop()
}
def func1(s: (String,Double)): String = { 
  	val e=s._1
    val f=s._2
    val c=e.toInt
    return c.toString+"\t"+f.toString
}
}
    



