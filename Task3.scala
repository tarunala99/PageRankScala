import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.lib.PageRank

object Task3{
def main(args: Array[String]): Unit = {
val spark = SparkSession.builder().appName("Broadcast Test").getOrCreate()
val sc = spark.sparkContext
val graph = GraphLoader.edgeListFile(sc,"s3://cmucc-datasets/TwitterGraph.txt")
val ranks = PageRank.run(graph,10)
val olderFollowers: VertexRDD[(Double, Double)] = ranks.aggregateMessages[(Double, Double)](triplet => {triplet.sendToDst(triplet.srcId, triplet.srcAttr)},(a,b) => (if(a._2 > b._2) a._1 else b._1,a._2 max b._2))
val can = ranks.vertices.map({case (x,y) => (x,y)}).subtractByKey(olderFollowers.map({case (x,y) => (x,1)}))
val contd = can.map({case (x,y) => (x,(0.00,0.00))})
val semin = olderFollowers.union(contd)
val ole=ranks.vertices.join(semin)
val newGraph=Graph(ole,ranks.edges)
val newFollowers: VertexRDD[(Double, Double)] = newGraph.aggregateMessages[(Double, Double)](triplet => {triplet.sendToDst(triplet.srcAttr._2._1, triplet.srcAttr._2._2)},(a,b) => (if(a._2 > b._2) a._1 else b._1,a._2 max b._2))
val ranks1 = newFollowers.map({case (x,(y,z)) => x.toString+"\t"+y.toInt.toString+"\t"+z.toString})
ranks1.saveAsTextFile("hdfs:///task3-output")
spark.stop()
}
}

