package comp9313.lab7

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SSSPExample {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("SSSP").setMaster("local")
    val sc = new SparkContext(conf)
	
    val fileName = args(0)    
    val sourceId = args(1).toLong
    val edges = sc.textFile(fileName)
    val edgelist = edges.map(x => x.split(" ")).map(x=> Edge(x(1).toLong, x(2).toLong, x(3).toDouble))	
    val graph = Graph.fromEdges[Double, Double](edgelist, 0.0)
    graph.triplets.collect()
    
	//compute the shortest distances here
    sc.stop()
  }
}