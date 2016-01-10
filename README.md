The goal of this project was to calculate the semantic similarity of terms in a document using Apache Spark & Scala.

## Project Steps

1.	Get the totalNumberOfDocuments count

2.	Flatmap each document and split the words in the document by whitespace
We chose this because we realized that we can get all of the information necessary for the tf-idf calculation in one step, optimizing and preventing reprocessing of each line.

3.	Filter the terms to match the gene format and map each term to the key as (term, documentID, totalWordsInDocument) , 1. This is the wordcount problem.

4.	MAP :  (term, documentID, totalWordsInDocument) => 1

5.	We reduce this by key to get the total wordCount for that term per document

6.	REDUCE:  (term, documentID, totalWordsInDocument) => wordCountForTerm

7.	ReMAP this such that : term => (documentID, totalWordsInDocument, wordCountForTerm) which is the rest of the info we need to calculate the tf-idf

8.	With the term being the new key, we can group by the key (REDUCE) to get a list of the proper values

9.	We map this such that we calculate a list of tf-idfs for each term for the proper documents

10.	MAP : Term => Iterable(documentID, tf-idf)

11.	We take the Cartesian of the two in order to get the pairs of terms but filter it by lexicographical difference to avoid duplicates

12.	We map each pair to their cosineSimilarity, pairNames and filter out those genes with 0 similarity since they don’t give us much useful information and speeds up the program

13.	 MAP : cosineSimilarity => (term1, term2)

14.	Sort by the cosine similarity in descending order. This also would allow us to group pairs with the same cosineSimilarity if we chose to do so.

## Spark on Amazon EC2 Process

1.	Set up 1 master node with 5 slaves of instance type t2.micro and it took roughly 10 minutes to complete the entire data set.

2.	We noticed an improvement of about 1 ½ minutes when getting all necessary tf-idf info in 1 step, filtering away duplicates from the Cartesian, and filtering away pairs with a cosine similarity of 0.
