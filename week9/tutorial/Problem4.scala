package comp9313.lab7

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem4 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("khops").setMaster("local")
    val sc = new SparkContext(conf)
    
    val fileName = args(0)
    val k = args(1).toInt
    val edges = sc.textFile(fileName)   
   
    val edgelist = edges.map(x => x.split(" ")).map(x=> Edge(x(1).toLong, x(2).toLong, x(3).toDouble))	
    val graph = Graph.fromEdges[Double, Double](edgelist, 0.0)
	  graph.triplets.collect().foreach(println)
    
    val initialGraph = graph.mapVertices((id, _) => Set[VertexId]())
    /* fill your code here
    val res = initialGraph.pregel ... ...
    */
       
    println(res.vertices.filter{case(id, attr) => attr.contains(id)}.collect().mkString("\n"))
    sc.stop()
  }
}