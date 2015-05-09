import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.math.log

object main {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Hello").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val gene = "gene_.*_gene".r.toString() // Our filter for gene_SOMETHING_gene format
   
    val lines = sc.textFile("project3.dat")
    val totalDocs = lines.count() //Counting the total number of documents
    
    //Word Count
    val counts =   lines.flatMap{
                         line => lazy val s = line.split("\\s+")
                         val count = s.length
                         val id = s.head
                         s.filter(word => word.matches(gene)).map(word => ((word, id, count), 1))
                   }.reduceByKey(_+_)
   // counts holds : (term, documentID, totalWordsInDocument) => wordCount
    //counts.foreach(x => println(x)) 
    
    val counts2 = counts.map( quad => (quad._1._1, (quad._1._2, quad._2, quad._1._3, 1)) ).reduceByKey((a,b) => (a._1,a._2,a._3,(a._4 + b._4)))
    //counts2 holds : term => (documentsID, wordCount, totalWordsInDocument, NumDocumentsTermAppearsIn)
    counts2.foreach(x => println(x))
    
    val counts3 = counts2.map(quad => ((quad._1,quad._2._1),((quad._2._2/quad._2._3.toDouble) * log(totalDocs/quad._2._4))))
    
    val counts4 = counts3.map(pair => (pair._1._1, (pair._1._2, pair._2,1))).reduceByKey((a,b)=> (a._1,a._2,(a._3 + b._3)))
    
    //counts4.foreach(x => println(x))
    
    
                     
                   
  }
}