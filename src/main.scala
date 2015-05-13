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
    
   // counts3.sortBy((a=> a._1),true, 10).foreach(x => println(x))
    
   val counts4 = counts3.cartesian(counts3).filter(pair => pair._1._1 > pair._2._1)
                 //.map(pair => ((pair._1._1,pair._2._1) , cosSimilarity(pair._1._2.toList,pair._2._2.toList) ))
   //FILTER BASED ON LEXICOGRAPHICAL DIFFERENCE TO GET ALL COMBINATIONS
   //COUNTS4 CONTAINS ALL OF THE PAIRS WHICH WE MUST CALCULATE THE COSINE SIMILARITY
   //counts4.foreach(x => println(x))
   
   val counts5 = counts4.map(pair => ((pair._1._1,pair._2._1) , cosSimilarity(pair._1._2.toList,pair._2._2.toList) ))
   counts5.foreach(x => println(x))
   
   //val list1: List[Tuple2[String,Double]] =  List( ("a",3), ("b",2), ("d",5), ("e",5) )
   //val list2: List[Tuple2[String,Double]] =  List( ("a",2), ("c",2), ("e",7) )
   
   //val x = cosSimilarity(list1,list2)
                   
  }


  def cosSimilarity(A:List[(String,Double)],B:List[(String,Double)]): Double = {//RDD[Tuple2[String,Double]] = {
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    println("near, far, wherever you are")
    
    val vecA = sc.parallelize(A)//.map(pair => (pair._1,pair._2))
      val vecB = sc.parallelize(B)//.map(pair => (pair._1,pair._2))
      
      val normA = pow(vecA.map(x => pow(x._2, 2)).sum , .5)
      val normB = pow(vecB.map(x => pow(x._2, 2)).sum , .5)
      
      //val dotProduct = vecA.union(vecB).reduceByKey(_*_)//.map(pair =>(pair._1,pair._2)).reduceByKey(_*_)
     // dotProduct.foreach(x => println(x))
      
      val dotProduct = vecA.cogroup(vecB).filter{
        case(vals) => (vals._2._1.size > 0 && vals._2._2.size > 0)
      }.map(a => (a._2._1.head * a._2._2.head))
      
      dotProduct.foreach(x => println(x))
      
      (dotProduct.sum / (normA * normB))
  
  
  }

}






