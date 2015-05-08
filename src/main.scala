import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer

object main {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Hello").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val gene = "gene_.*_gene".r.toString() // Our filter for gene_SOMETHING_gene format
   
    val lines = sc.textFile("project3.dat")
    val totalDocs = lines.count() //Counting the total number of documents
    
    var docID = "" //temporary value for document id
      
    var words = new ListBuffer[String]
    var all = new ListBuffer[String]
    var realAll = sc.parallelize(all)

    
    //Word Count

    
    val counts =   lines.flatMap{
                         line => lazy val s = line.split("\\s+")
                         val id = s.head
                         s.filter(word => word.matches(gene)).map(word => ((word, id), 1))
                   }.reduceByKey(_+_)

    counts.foreach(x => println(x)) 
  }
  
}