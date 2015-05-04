import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Hello").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val gene = "gene_.*_gene".r.toString()
   
    val lines = sc.textFile("project3.dat")
    
    val counts = lines.flatMap(line => line.split("\\s+").filter { x => x.matches(gene) })
         .map(word => (word, 1))
         .reduceByKey(_ + _)
         
         counts.foreach(x => println(x))  
  }
}