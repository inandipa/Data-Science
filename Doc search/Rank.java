

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

public class Rank extends Configured implements Tool {
   private static final Logger LOG = Logger .getLogger( Rank.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Rank(), args);
      System .exit(res);
   }

     public int run( String[] args) throws  Exception {

       String param = "";
	   for(int i =2 ; i<args.length ; i++){		/* if at all user enter any arguments more than 2, they are stored in */
		if(i==2){				/*variable seperated by <space> for search query*/
		param = args[2];		
		}else{
		   param = param+" "+args[i];
		}	
	   }
	     Job TF  = Job .getInstance(getConf(), " TF ");		/*new job is created for finding term frequency*/
	     TF.setJarByClass( this .getClass());
		Configuration conf = getConf();
	   FileSystem Fs = FileSystem.get(conf);
	     final int totalFiles = Fs.listStatus(new Path(args[0])).length;  	/*number of files in input location is found using */
  	   conf.set("DOC_NUM", String.valueOf(totalFiles));			/*inbuilt function*/
	     conf.set("query", param);
	     FileInputFormat.addInputPaths(TF,  args[0]);
	     FileOutputFormat.setOutputPath(TF,  new Path("OUT_TF_RESULT"));	/* input location is taken from first argument*/
	      									/* its result are stored in OUT_TF_RESULT location */
     TF.setMapperClass( TF_MAP .class);
     TF.setReducerClass( TF_Reduce .class);
     TF.setOutputKeyClass( Text .class);
     TF.setOutputValueClass( IntWritable .class);
     TF.waitForCompletion( true);
     Job IDF  = Job .getInstance(getConf(), " IDF ");
     IDF.setJarByClass( this .getClass());					/* input location is taken from OUT_TF_RESULT*/
	      									/* its result are stored in OUT_IDF_RESULT location */
     FileInputFormat.addInputPaths(IDF,  "OUT_TF_RESULT");
     FileOutputFormat.setOutputPath(IDF,  new Path("OUT_IDF_RESULT"));
    
     IDF.setMapperClass( IDF_MAP .class);
     IDF.setReducerClass( IDF_Reduce .class);
     IDF.setOutputKeyClass( Text .class);
     IDF.setOutputValueClass( Text .class);
     
	if(param == ""){					/* checking if user entered search query if not seach is notcalled */
	 return IDF.waitForCompletion( true)  ? 0 : 1;		
	}else{
	IDF.waitForCompletion(true)  ;
     Job Search  = Job .getInstance(getConf(), " Search ");
     Search.setJarByClass( this .getClass());					/* input location is taken from OUT_IDF_RESULT*/
	      								/* its result are stored in OUT_SEARCH_RESULT location */
     FileInputFormat.addInputPaths(Search,  "OUT_IDF_RESULT");
     FileOutputFormat.setOutputPath(Search,  new Path("OUT_Search_RESULT"));
    
     Search.setMapperClass( Search_MAP .class);
     Search.setReducerClass( Search_Reduce .class);
     Search.setOutputKeyClass( Text .class);
     Search.setOutputValueClass( DoubleWritable .class);

	
     Search.waitForCompletion( true)  ;
    Job Rank  = Job .getInstance(getConf(), " Rank ");
     Rank.setJarByClass( this .getClass());

     FileInputFormat.addInputPaths(Rank, "OUT_Search_RESULT");  
     FileOutputFormat.setOutputPath(Rank,  new Path(args[1]));  /*final rank result are stored in output path*/    
     Rank.setMapperClass( Rank_MAP .class);
     Rank.setReducerClass( Rank_Reduce .class);   /*Rank Job*/
     Rank.setOutputKeyClass( Text .class);
     Rank.setOutputValueClass( Text .class);
    	
     return Rank.waitForCompletion( true)  ? 0 : 1;		/*after executing all mapreduce functions code exit with code 0*/
	}
  }
  
  public static class TF_MAP extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
     private final static IntWritable one  = new IntWritable( 1);

     private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

     public void map( LongWritable offset,  Text lineText,  Context context)
       throws  IOException,  InterruptedException {		/* code is vary similar to wordcount*/

        String line  = lineText.toString();
        Text currentWord  = new Text();
        FileSplit file = (FileSplit) context.getInputSplit();
        String fileName = file.getPath().getName().toString();


        for ( String word  : WORD_BOUNDARY .split(line)) {
           if (word.isEmpty()) {
              continue;
           }
           word = word+"####"+fileName;			/*word is concated with their filename with #### in middle in between them*/
           currentWord  = new Text(word);
           context.write(currentWord,one);
        }
     }
  }
	public static class Search_MAP extends Mapper<LongWritable ,Text ,Text ,DoubleWritable > {
    	
   	 private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");
	 private String key,vaule;

    	 public void map( LongWritable offset,  Text lineText,  Context context)
       	throws  IOException,  InterruptedException {
    		 Configuration conf = context.getConfiguration();	/* search query is fetched from config,which was set in run */
             String param = conf.get("query");				/* it is then separated based on <space> between them*/
             String line  = lineText.toString();
             String[] lines = line.split(System.getProperty("line.separator"));

		       for ( String string : lines) {			/*each of these word is checked with word in IDF output file*/
			for ( String word  : WORD_BOUNDARY .split(param)) {
 		          if (word.isEmpty()) {
  		            continue;
   		        }
       		    key = string.split("####")[0];		/*if its matched its forwarded to output in required format*/
       		    if(key.equals(word)){
    	    	   	String currentkey = string.split("####")[1].split("	")[0];
       		    	vaule = string.split("####")[1].split("	")[1];
       		    	double currentvaule  = Double.valueOf(vaule);
     			context.write(new Text(currentkey),new DoubleWritable(currentvaule));
       		
       			    }
		           
				}
		           
		       }
    	 }
	}


 public static class IDF_MAP extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		    
		      private String key,vaule;
		      public void map( LongWritable offset,  Text lineText,  Context context)
		        throws  IOException,  InterruptedException {

		         String line  = lineText.toString();
		         Text currentkey  = new Text();
		         Text currentvaule  = new Text();
		         
		         String[] lines = line.split(System.getProperty("line.separator"));

		         for ( String string : lines) {		/*each word is fetched and processed and output in required form*/
		            key = string.split("####")[0];
		            vaule = string.split("####")[1].replace('	', '=');
		            currentkey  = new Text(key);
		            currentvaule  = new Text(vaule);
		            
		            context.write(currentkey,currentvaule);
		         }
		      }
		   }


public static class Rank_MAP extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
        
          public void map( LongWritable offset,  Text lineText,  Context context)
            throws  IOException,  InterruptedException {

             String line  = lineText.toString();
             Text currentkey  = new Text("DES");		/*input is taken and changed that its able to process in reducer*/
             String[] lines = line.split(System.getProperty("line.separator"));

             for ( String string : lines) {   
          string = string.replace(' ', '=');
                context.write(currentkey,new Text(string));
             }
          }
       }

  
 public static class Search_Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
     @Override 
     public void reduce( Text word,  Iterable<DoubleWritable > list,  Context context)
        throws IOException,  InterruptedException {
        double sum  = 0;
        for ( DoubleWritable count  : list) {
           sum  += count.get();			/* this is default reducer to find wordcount*/
        }
         
        context.write(word,  new DoubleWritable(sum));
     }
  }
  
  
  public static class TF_Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
     @Override 
     public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
        throws IOException,  InterruptedException {
        int sum  = 0;
        for ( IntWritable count  : counts) {
           sum  += count.get();
        }
         double tf = (double) (1+ Math.log10(sum)) ; 		/* simple TF formula is applied on count to find TermFrequency*/
         
        context.write(word,  new DoubleWritable(tf));
     }
  }

 public static class IDF_Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
	   
		 @Override 
	    public void reduce( Text word,  Iterable<Text > list,  Context context)
	       throws IOException,  InterruptedException {
	    	
	    Configuration conf = context.getConfiguration();	/*Doc number is obtained from config  which is set in run method*/
           String param = conf.get("DOC_NUM");
           int N = Integer.parseInt(param);			/*new array list is created to store th values for each word*/
           ArrayList<Text> cache = new ArrayList<Text>();	/*because once loop run over iterable all its values are deleted*/
           for (Text aNum : list) {				/*This arry list is used to find size of array list*/
		Text text = new Text();
               text.set(aNum.toString());
               cache.add(text);
           }
           int size = cache.size();				/*output is sent as per requirement*/
           for (int i = 0; i < size; i++) {
		double idf = Math.log10(1+(N/size));
           	String key = cache.get(i).toString().split("=")[0];
           	String vaule = cache.get(i).toString().split("=")[1];
		double tf = Double.valueOf(vaule);
		double tf_idf = tf*idf;
           	key = word.toString()+"####"+key;
           	  
               context.write(new Text(key),  new DoubleWritable(tf_idf));
            
              }
	    }
	 }

public static class Rank_Reduce extends Reducer<Text ,Text ,  Text ,DoubleWritable > {
     @Override 
     public void reduce( Text word,  Iterable<Text > list,  Context context)
        throws IOException,  InterruptedException {
        TreeMap<Double, String> tm = new TreeMap<Double,String>(Collections.reverseOrder());
         for (Text aNum : list) {   /*enter each input into treemap which automatically sort the input*/
            Text text = new Text();
                       text.set(aNum.toString());
                      String key = text.toString().split("=")[0];
                      double value = Double.valueOf(text.toString().split("=")[1]);
                      tm.put(value, key);
                   }
         Set<Entry<Double, String>> set = tm.entrySet();
           
           Iterator<Entry<Double, String>> i = set.iterator();
           
           while(i.hasNext()) { /*print the key and values of tree, as they are in decreasing order*/
              Map.Entry<Double, String> me = (Map.Entry<Double, String>)i.next();
              context.write(new Text((String) me.getValue()),  new DoubleWritable((Double) me.getKey()));
           }
     }
  }
  
  
}
