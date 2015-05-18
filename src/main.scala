import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.math.log10
import scala.math.pow

object main {
 
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Project_3").setMaster("local[*]");
    val sc = new SparkContext(conf)
    
    val gene = "gene_.*_gene".r.toString() // Our filter for gene_SOMETHING_gene
   
    val lines = sc.textFile(args(0))

    val totalDocs = lines.count() //Get |D| (total number of documents)

    val info = lines.flatMap {
                         line => lazy val words = line.split("\\s+")
                         val filteredWords = words.filter(term => term.matches(gene))
                         filteredWords.map(term => ( (term, words.head, filteredWords.length ), 1) )
                         //words.filter(term => term.matches(gene)).map(term => ( (term, words.head, words.length-1 ), 1) )
                         
                   }.reduceByKey(_+_)
    //info contains : (term, documentID, totalDocumentWordCount) => wordCountForTerm

    //tfIdfInfo remaps the info to make term the key
    val tfIdfInfo = info.map( info => ( info._1._1, (info._1._2, info._1._3, info._2 )))
    // It contains : term => (documentID, totalDocumentWordCount, wordCountForTerm)
    
   //termTfIdf calculates each term's tf/idf  per document based on tfIdfInfo and groups them by the key (term)
    val termTfIdf = tfIdfInfo.groupByKey()
                             .mapValues(array => array.map(triple => (triple._1, ( (triple._3/triple._2.toDouble) * log10(totalDocs/array.size.toDouble) ) )))//.filter(x => x._2 > 0)).filter(x => x._2.size > 0)
    //It contains : term => List[documentID, tf/idf] 

   //We get all permutations of terms and filter out duplicates based on lexicographical difference giving us the distinct combinations of terms
    val termPairs = termTfIdf.cartesian(termTfIdf).filter(pair => pair._1._1 > pair._2._1)
    //We now have all the information needed to calculate the cosine similarity between pairs 

    // We map each term pair to their cosine similarity and pair of term names, filtering out the ones with 0 similarity since most will be 0 and don't give us useful information
    val cosSimPairs =  termPairs.map(pair => (cosSimilarity(pair._1._2,pair._2._2),(pair._1._1,pair._2._1)))
                                .filter(x=> x._1 > 0.0)
   
    //Sort the pairs, collect them  to output their true sorted order for viewing
    cosSimPairs.sortByKey(false).collect.foreach( termPair => println(termPair))
  }
  
  //Returns the cosine Similarity of two Iterables
  def cosSimilarity(A:Iterable[(String,Double)],B:Iterable[(String,Double)]): Double  = {
    dotProduct(A,B) / productOfNorms(A,B)
  }
  
  //Returns the product of the norms of two Iterables
  def productOfNorms(A:Iterable[(String,Double)],B:Iterable[(String,Double)]): Double = {
    getNorm(A) * getNorm(B)
  }
  //Returns the norm of an Iterable (Euclidean Distance)
  def getNorm(vector: Iterable[(String,Double)]): Double = {
    pow(vector.map(x => pow(x._2, 2)).sum, .5)
  }

  //Returns the dot product of two Iterables
  def dotProduct(A:Iterable[(String,Double)],B:Iterable[(String,Double)]): Double = {
    /* Append the Iterables to each other and group them by the key (document ID) and get only the values in the Iterable
       where there are at least 2. This will in turn optimize the dot product calculation since we only take the product of
       matching non-zero values */
    val dotProduct = (A ++ B).groupBy(_._1).mapValues(_.map(_._2)).filter(_._2.size > 1)
                     .map(x => x._2.product)
      
      dotProduct.sum 
  }
}