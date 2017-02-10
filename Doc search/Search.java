

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.util.ArrayList;
import java.util.Collection;
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


public class Search extends Configured implements Tool {
   private static final Logger LOG = Logger .getLogger( Search.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Search(), args);
      System .exit(res);
   }

     public int run( String[] args) throws  Exception {

       String param = "";
	   for(int i =2 ; i<args.length ; i++){
		if(i==2){
		param = args[2];		
		}else{
		   param = param+" "+args[i];
		}	
	   }
	     Job TF  = Job .getInstance(getConf(), " TF ");
	     TF.setJarByClass( this .getClass());
		Configuration conf = getConf();
	   FileSystem Fs = FileSystem.get(conf);
	     final int totalFiles = Fs.listStatus(new Path(args[0])).length;  	
  	   conf.set("DOC_NUM", String.valueOf(totalFiles));
	     conf.set("query", param);
	     FileInputFormat.addInputPaths(TF,  args[0]);
	     FileOutputFormat.setOutputPath(TF,  new Path("OUT_TF_RESULT"));
	      
     TF.setMapperClass( TF_MAP .class);
     TF.setReducerClass( TF_Reduce .class);
     TF.setOutputKeyClass( Text .class);
     TF.setOutputValueClass( IntWritable .class);
     TF.waitForCompletion( true);
     Job IDF  = Job .getInstance(getConf(), " IDF ");
     IDF.setJarByClass( this .getClass());

     FileInputFormat.addInputPaths(IDF,  "OUT_TF_RESULT");
     FileOutputFormat.setOutputPath(IDF,  new Path("OUT_IDF_RESULT"));
    
     IDF.setMapperClass( IDF_MAP .class);
     IDF.setReducerClass( IDF_Reduce .class);
     IDF.setOutputKeyClass( Text .class);
     IDF.setOutputValueClass( Text .class);
     
	if(param == ""){
	 return IDF.waitForCompletion( true)  ? 0 : 1;
	}else{
	IDF.waitForCompletion(true)  ;
     Job Search  = Job .getInstance(getConf(), " Search ");
     Search.setJarByClass( this .getClass());

     FileInputFormat.addInputPaths(Search,  "OUT_IDF_RESULT");
     FileOutputFormat.setOutputPath(Search,  new Path(args[1]));
    
     Search.setMapperClass( Search_MAP .class);
     Search.setReducerClass( Search_Reduce .class);
     Search.setOutputKeyClass( Text .class);
     Search.setOutputValueClass( DoubleWritable .class);
    
     return Search.waitForCompletion( true)  ? 0 : 1;
	}
  }
  
  public static class TF_MAP extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
     private final static IntWritable one  = new IntWritable( 1);

     private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

     public void map( LongWritable offset,  Text lineText,  Context context)
       throws  IOException,  InterruptedException {

        String line  = lineText.toString();
        Text currentWord  = new Text();
        FileSplit file = (FileSplit) context.getInputSplit();
        String fileName = file.getPath().getName().toString();


        for ( String word  : WORD_BOUNDARY .split(line)) {
           if (word.isEmpty()) {
              continue;
           }
           word = word+"####"+fileName;
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
    		 Configuration conf = context.getConfiguration();
             String param = conf.get("query");
             String line  = lineText.toString();
             String[] lines = line.split(System.getProperty("line.separator"));

		       for ( String string : lines) {
			for ( String word  : WORD_BOUNDARY .split(param)) {
 		          if (word.isEmpty()) {
  		            continue;
   		        }
       		    key = string.split("####")[0];
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

		         for ( String string : lines) {
		            key = string.split("####")[0];
		            vaule = string.split("####")[1].replace('	', '=');
		            currentkey  = new Text(key);
		            currentvaule  = new Text(vaule);
		            
		            context.write(currentkey,currentvaule);
		         }
		      }
		   }
  
 public static class Search_Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
     @Override 
     public void reduce( Text word,  Iterable<DoubleWritable > list,  Context context)
        throws IOException,  InterruptedException {
        double sum  = 0;
        for ( DoubleWritable count  : list) {
           sum  += count.get();
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
         double tf = (double) (1+ Math.log10(sum)) ;
         
        context.write(word,  new DoubleWritable(tf));
     }
  }

 public static class IDF_Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
	   
		 @Override 
	    public void reduce( Text word,  Iterable<Text > list,  Context context)
	       throws IOException,  InterruptedException {
	    	
	    Configuration conf = context.getConfiguration();
           String param = conf.get("DOC_NUM");
           int N = Integer.parseInt(param);
           ArrayList<Text> cache = new ArrayList<Text>();
           for (Text aNum : list) {
		Text text = new Text();
               text.set(aNum.toString());
               cache.add(text);
           }
           int size = cache.size();
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
  
  
}
