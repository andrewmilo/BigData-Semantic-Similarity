import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.math.log
import scala.math.pow

object main {
  val conf = new SparkConf().setAppName("Project_3").setMaster("local[*]");
  val sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {
    
    val gene = "gene_.*_gene".r.toString() // Our filter for gene_SOMETHING_gene
   
    val lines = sc.textFile("project3.dat",6)
    
    val totalDocs = lines.count() //Counting the total number of documents
      
    //Word Count
    val counts = lines.flatMap {
                         line => lazy val words = line.split("\\s+")
                         words.filter(term => term.matches(gene)).map(term => ( (term, words.head, words.length ), 1) )//.map(word => ( (word: term, ((id: lineID, totalCount: totalWordCount):Document , 1: countInDoc) )))
                   }.reduceByKey(_+_)
    //COUNTS CONTAINS (term, documentID, totalDocumentWordCount) => wordCountForTerm

    val counts2 = counts.map( quad => ( quad._1._1, (quad._1._2, quad._1._3, quad._2 )))
    //COUNT2 REMAPS COUNTS AND CONTAINS term => (documentID, totalDocumentWordCount, wordCountForTerm)
    
    val counts3 = counts2.groupByKey().mapValues(array => array.map(pair => (pair._1, (pair._3/pair._2.toDouble) * log(totalDocs/array.size))))
    //COUNTS3 GROUPS COUNTS2 BY KEY, calculates tf/idf and CONTAINS term => List[documentID, tf/idf]
    
   // counts3.sortBy((a=> a._1),true, 10).foreach(x => println(x))
    
   val counts4 = counts3.cartesian(counts3).filter(pair => pair._1._1 > pair._2._1)
                 //.map(pair => ((pair._1._1,pair._2._1) , cosSimilarity(pair._1._2.toList,pair._2._2.toList) ))
   //FILTER BASED ON LEXICOGRAPHICAL DIFFERENCE TO GET ALL COMBINATIONS
   //COUNTS4 CONTAINS ALL OF THE PAIRS WHICH WE MUST CALCULATE THE COSINE SIMILARITY
   
   //val withNorms = counts4.map(pair => ((pair._1._1,pair._2._1), productOfNorms(pair._1._2,pair._2._2)))
  // withNorms.foreach(x=>println(x))
   
   val withDotProduct =  counts4.map(pair => (cosSimilarity(pair._1._2,pair._2._2),(pair._1._1,pair._2._1))).filter(x=> x._1 > 0.0)
  // withDotProduct.cache
   withDotProduct.sortByKey().collect.foreach(x=>println(x))
  // println(withDotProduct.count())

  }
  
  def cosSimilarity(A:Iterable[(String,Double)],B:Iterable[(String,Double)]): Double = {
    dotProduct(A,B)/productOfNorms(A,B)
  }
  
  def productOfNorms(A:Iterable[(String,Double)],B:Iterable[(String,Double)]): Double = {
    getNorm(A) * getNorm(B)
  }
  
  def getNorm(vector: Iterable[(String,Double)]): Double = {
    pow(vector.map(x => pow(x._2, 2)).sum, .5)
  }

  def dotProduct(A:Iterable[(String,Double)],B:Iterable[(String,Double)]): Double = {
    /*
      A.foreach(x=>println(x))
      println("-------------------------------------------")
      B.foreach(x=>println(x))
*/
      val dotProduct = (A ++ B).groupBy(_._1).mapValues(_.map(_._2)).filter{
        
        case(vals) => (vals._2.size > 1)
        
      }
      .map(x=>x._2.product)
      /*val dotProduct = A.cogroup(B).filter{
        case(vals) => (vals._2._1.size > 0 && vals._2._2.size > 0)
      }.map(a => (a._2._1.head * a._2._2.head))
      
      
*/
      //println("I'm here now: ", dotProduct.size)
      //dotProduct.foreach(x=>println(x))
      //println(dotProduct.sum)
      return dotProduct.sum
   
  }

}