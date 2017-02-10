
All the programs are implemented and expected results are obtained.
Please check the Search output first and then proceed with rank.

As default, change working directory to /user/<username>/
(Note- intermediate output values are stored according to this directory.).

/*put input files in directory*/ 
hadoop fs -put file* /user/<username>/IN

And move all the java file to the working directory. Comments are specified in rank.java file, as it covers all the code.

There are total 4  map reduce works in Rank.java. First is for Termfrequency and the result of it is stored in OUT_TF_RESULT. its output is again used for TFIDF and subsequent result is stored in OUT_IDF_RESULT.This is then by Search and match the total words with query and return the file name and its coresponding weight in location OUT_Search_RESULT.Rank progrsm just sort the files list according to score and store the final output in OUT. IF at all we dont pass pass any arguments as query to search, then search progrm in not all executed it stops after finding TFIDF values.



<!-----------------------------------------------------------------------------------------------------!>
Running Commands - 


sudo hadoop fs -rm -r /user/<username>/OUT*
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Search.java  (run from directory having java file).
jar -cvf Search.jar .
sudo hadoop jar Search.jar Search /user/<uername>/IN /user/<username>/OUT <term> <term>


(note- we can enter many search terms divided by <space>).


<!-----------------------------------------------------------------------------------------------------!>

Rank.java is the final java file, it does have all code of TF,IDF,Search and Rank. Executing this file will produce output as,

TermFrequency output is stored in /user/<username>/OUT_TF_RESULT

IDF output is stored in /user/<username>/OUT_IDF_RESULT

Rank output is stored in /user/<username>/OUT

command to see output - 

hadoop fs -cat /user/<username>/OUT/part-r-*









<!-----------------------------------------------------------------------------------------------------!>

Corresponding output for these programs will be in /user/<username>/OUT

Rank Commands - 



sudo hadoop fs -rm -r /user/<username>/OUT*
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Rank.java 
jar -cvf Rank.jar .
sudo hadoop jar Rank.jar Rank /user/root/IN /user/root/OUT <term>



TF-IDF Commands - 


sudo hadoop fs -rm -r /user/<username>/OUT*
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TFIDF.java 
jar -cvf TFIDF.jar .
sudo hadoop jar TFIDF.jar TFIDF /user/<username>/IN /user/<username>/OUT


TF COmmands - 


sudo hadoop fs -rm -r /user/<username>/OUT*
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java 
jar -cvf TermFrequency.jar .
sudo hadoop jar TermFrequency.jar TermFrequency /user/<username>/IN /user/<username>/OUT

DocWordCount Commands - 


sudo hadoop fs -rm -r /user/root/OUT*
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java 
jar -cvf DocWordCount.jar .
sudo hadoop jar DocWordCount.jar DocWordCount /user/<username>/IN /user/<username>/OUT



