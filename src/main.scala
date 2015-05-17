import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.math.log
import scala.math.pow

object main {
 
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Project_3").setMaster("local[4]");
    val sc = new SparkContext(conf)
    
    val gene = "gene_.*_gene".r.toString() // Our filter for gene_SOMETHING_gene
   
    val lines = sc.textFile("project3.dat")

    val totalDocs = lines.count() //Get |D| (total number of documents)
    
    val info = lines.flatMap {
                         line => lazy val words = line.split("\\s+")
                         words.filter(term => term.matches(gene)).map(term => ( (term, words.head, words.length ), 1) )//.map(word => ( (word: term, ((id: lineID, totalCount: totalWordCount):Document , 1: countInDoc) )))
                   }.reduceByKey(_+_)
    //info contains : (term, documentID, totalDocumentWordCount) => wordCountForTerm

    val tfIdfInfo = info.map( info => ( info._1._1, (info._1._2, info._1._3, info._2 )))
    //tfIdfInfo remaps the info and now contains : term => (documentID, totalDocumentWordCount, wordCountForTerm)
    
    val termTfIdf = tfIdfInfo.groupByKey().mapValues(array => array.map(pair => (pair._1, (pair._3/pair._2.toDouble) * log(totalDocs/array.size))))
    /*termTfIdf calculates each term's tf/idf  per document based on tfIdfInfo and groups them by the key(term)
      It contains : term => List[documentID, tf/idf] */
    
   val termPairs = termTfIdf.cartesian(termTfIdf).filter(pair => pair._1._1 > pair._2._1)
   /*We get all combinations of terms and filter out duplicates based on lexicographical difference
     We now have all the information needed to calculate the cosine similarity between pairs */

   val cosSimPairs =  termPairs.map(pair => (cosSimilarity(pair._1._2,pair._2._2),(pair._1._1,pair._2._1))).filter(x=> x._1 > 0.0)
   // We map each term pair to their cosine similarity and pair of term names, filtering out the ones with 0 similarity since most will be 0 and don't give us useful information
   cosSimPairs.sortByKey().collect.foreach(x=>println(x))
  }
  
  def cosSimilarity(A:Iterable[(String,Double)],B:Iterable[(String,Double)]): Double = {
    val dProd = dotProduct(A,B)
    val normProd = productOfNorms(A,B)
    dProd/normProd
  }
  
  def productOfNorms(A:Iterable[(String,Double)],B:Iterable[(String,Double)]): Double = {
    val normA = getNorm(A)
    val normB = getNorm(B)
    normA * normB
  }
  
  def getNorm(vector: Iterable[(String,Double)]): Double = {
    pow(vector.map(x => pow(x._2, 2)).sum, .5)
  }

  def dotProduct(A:Iterable[(String,Double)],B:Iterable[(String,Double)]): Double = {
      val dotProduct = (A ++ B).groupBy(_._1).mapValues(_.map(_._2)).filter{     
        case(vals) => (vals._2.size > 1)
      }
      .map(x=>x._2.product)
      
      dotProduct.sum 
  }
}