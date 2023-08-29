package comp9313.lab7
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object VotesStat {

  def Question2(textFile: DataFrame, spark: SparkSession) {
    import spark.implicits._
    // write your code here
  }
  
  def Question1(textFile: DataFrame, spark: SparkSession) {
    import spark.implicits._
    //write your code here  
  }


  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val spark = SparkSession.builder.appName("VotesStat").getOrCreate()
    val textFile = spark.read.csv(inputFile).toDF("Id", "PostId", "VoteTypeId", "UserId", "CreationTime")
    
    Question1(textFile, spark)
    Question2(textFile, spark)
    spark.stop()
  }
}


