package comp9313.lab7

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Problem3 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("minValue").setMaster("local")
    val sc = new SparkContext(conf)
    
    // Create an RDD for the vertices
    val vertices: RDD[(VertexId, (Int, Int))] =
    sc.parallelize(Array((1L, (7,-1)), (2L, (3,-1)),
                       (3L, (2,-1)), (4L, (6,-1))))

    // Create an RDD for edges
    val relationships: RDD[Edge[Boolean]] =
    sc.parallelize(Array(Edge(1L, 2L, true), Edge(2L, 1L, true), Edge(1L, 4L, true), Edge(4L, 1L, true),
                       Edge(2L, 4L, true), Edge(4L, 2L, true), Edge(3L, 1L, true), Edge(1L, 3L, true), 
                       Edge(3L, 4L, true), Edge(4L, 3L, true)))

    // Create the graph
    val graph = Graph(vertices, relationships)

    // Check the graph
    graph.triplets.collect().foreach(println)

    val initialMsg = Int.MaxValue

    /* fill your code here
    def vprog(vertexId: VertexId, value: (Int, Int), message: Int): (Int, Int) = {
        ... ...
    }

    def sendMsg(triplet: EdgeTriplet[(Int, Int), Boolean]): Iterator[(VertexId, Int)] = {
        ... ...
    }

    def mergeMsg(msg1: Int, msg2: Int): Int = {
        ... ...
    }
    */

    val minGraph = graph.pregel(initialMsg, 
                            Int.MaxValue, 
                            EdgeDirection.Out)(
                            vprog,
                            sendMsg,
                            mergeMsg)
       
    minGraph.vertices.collect.foreach{case (vertexId, (value, original_value)) => println(value)}
    sc.stop()
  }
}
