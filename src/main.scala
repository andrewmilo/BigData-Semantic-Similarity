import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.math.log

object main {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Project_3").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val gene = "gene_.*_gene".r.toString() // Our filter for gene_SOMETHING_gene
   
    val lines = sc.textFile("project3.dat")
    
    val totalDocs = lines.count() //Counting the total number of documents
    
    
    type Document = (String, Int)
    type term = String
    type lineID = String
    type totalWordCount = Int
    type countInDoc = Int
  
    //Word Count
    val counts = lines.flatMap {
                         line => lazy val s = line.split("\\s+")
                         val totalCount = s.length
                         val id = s.head
                         
                         s.filter(word => word.matches(gene)).map( word => ( (word: term, (id: lineID, totalCount: totalWordCount):Document ), 1: countInDoc) )//.map(word => ( (word: term, ((id: lineID, totalCount: totalWordCount):Document , 1: countInDoc) )))
                   }.reduceByKey(_+_)
    //COUNTS CONTAINS (term, documentID, totalDocumentWordCount) => wordCountForTerm

    val counts2 = counts.map( quad => ( quad._1._1, ((quad._1._2._1, quad._1._2._2): Document, quad._2 )))
    //COUNT2 REMAPS COUNTS AND CONTAINS term => (documentID, totalDocumentWordCount, wordCountForTerm)
    
    val counts3 = counts2.groupByKey().mapValues(array => array.map(pair => (pair._1._1, (pair._2/pair._1._2.toDouble) * log(totalDocs/array.size))))
    //COUNTS3 GROUPS COUNTS2 BY KEY, calculates tf/idf and CONTAINS term => List[documentID, tf/idf]
    
    counts3.sortBy((a=> a._1),true, 10).foreach(x => println(x))
    
   val counts4 = counts3.cartesian(counts3)
   //COUNTS4 CONTAINS ALL OF THE PAIRS WHICH WE MUST CALCULATE THE COSINE SIMILARITY
                   
  }
  
  def cosSimilarity(A:List[Tuple2[String,Double]],B:List[Tuple2[String,Double]]): Unit = {
      val sc = new SparkContext()
      val vecA = sc.parallelize(A)
      val vecB = sc.parallelize(B)
      
      val x = vecA.flatMap(pair => pair._1)
  }
  
  
  
}







