package indra.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;



public class page_rank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( page_rank.class);
   private static final Pattern Title_BOUNDARY = Pattern .compile("<title>(.*?)</title>");		//regex for title of each line
   private static final Pattern Body_BOUNDARY = Pattern .compile("<text(.*?)</text>");			//regex for text of each line
   private static final Pattern Link_BOUNDARY = Pattern .compile("\\[\\[(.*?)\\]\\]");			////regex for each link in the text body
   private static final double  damp_factor = 0.85;

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new page_rank(), args);
      System .exit(res);
   }
   
   public int run( String[] args) throws  Exception {
      
	   Job job  = Job .getInstance(getConf(), " wordcount ");
      job.setJarByClass( this .getClass());
      
      																	// Job for assigning  initial rank to each title and formatting the required form
      FileInputFormat.addInputPaths(job,  args[0]);						//intermediate data re stored in /rank_pagei
      FileOutputFormat.setOutputPath(job,  new Path("intermediate/rank_page0"));
      job.setMapperClass( Map .class);									//all the inputs and outputs are stored in Text format.
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( Text .class);
      
       job.waitForCompletion( true) ;
      
       String in_path = "intermediate/rank_page0";
       String out_path = "";
       int y;
  
    	 for(int i = 0; i <10; i++){								//looping the process job for 10  times.
    		 y=i+1;													//data are stored in files system and fetched from that location recursively
    		 in_path = "intermediate/rank_page"+i;
    	      out_path = "intermediate/rank_page"+y;
    	  Job process  = Job .getInstance(getConf(), " process ");
          process.setJarByClass( this .getClass());

          FileInputFormat.addInputPaths(process,  in_path);
          FileOutputFormat.setOutputPath(process,  new Path(out_path));
          process.setMapperClass( Map_Process .class);
          process.setReducerClass( Reduce_Process .class);
          process.setOutputKeyClass( Text .class);
          process.setOutputValueClass( Text .class);
          process.waitForCompletion(true);
    	 }
      																		//cleanup job is to show the output in required format and delete all intermediate files
    	 
      	Job cleanup  = Job .getInstance(getConf(), " cleanup ");
      	cleanup.setJarByClass( this .getClass());

        FileInputFormat.addInputPaths(cleanup,  out_path);
        FileOutputFormat.setOutputPath(cleanup,  new Path(args[1]));
        cleanup.setMapperClass( Map_Clean .class);
        cleanup.setReducerClass( Reduce_Clean .class);
        cleanup.setOutputKeyClass( Text .class);
        cleanup.setOutputValueClass( Text .class);
        cleanup.waitForCompletion(true);

      return cleanup.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
   
      private static final Pattern Line_BOUNDARY = Pattern .compile("\\r?\\n");
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
         String data  = lineText.toString();						//dividing input text separated by new line and sent to single reducer
         															//if there are empty lines that are skipped
         for ( String line  : Line_BOUNDARY .split(data)) {			//all the lines are sent to reducer(count)
             if (line.isEmpty()) {
                continue;
             }
                 context.write(new Text("count"), new Text(line)); 
          }
         
      }
      
   }

   public static class Map_Process extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	   
	      private static final Pattern Line_BOUNDARY = Pattern .compile("\\r?\\n");
	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {			//this has input from file sent from first reducer and in format name####rank 	linl$@#$inlk2$@#$.....
	    	  String data  = lineText.toString();					//replace tab by ####
	    	  data = data.replaceAll("	", "####");
	         
	         for ( String line  : Line_BOUNDARY .split(data)) {
	             if (line.isEmpty()) {
	                continue;									//if the line empty don.t process.
	             }
	               String[] pre_data = line.split("####");		//split each line by #### so we get name,rank and links in array. 
	               if (pre_data.length <3) {
	            	   context.write(new Text(pre_data[0]), new Text(pre_data[1]+"####"+""));
		                continue;
		             }
	               double pre_rank = Double.valueOf(pre_data[1]);			//apply page rank algorithm here and send name and its corresponding link for line 
	               StringTokenizer st = new StringTokenizer(pre_data[2],"$@#$");

	              int N = st.countTokens();
	               while (st.hasMoreElements()) {
	            	   context.write(new Text(st.nextElement().toString()), new Text(String.valueOf(pre_rank/N)));
	       		}
	              
	               context.write(new Text(pre_data[0]), new Text(pre_data[1]+"####"+pre_data[2]));		//also send present rank and links for that page for further processing
	           
	          }
	         
	      }
	      
	   }
   
   public static class Map_Clean extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	   
	      private static final Pattern Line_BOUNDARY = Pattern .compile("\\r?\\n");
	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	    	  String data  = lineText.toString();
	    	
	         for ( String line  : Line_BOUNDARY .split(data)) {			// it just gets all data from file and send to one reducer line by line.
	             	if (line.isEmpty() ) {
	                continue;
	             }
	                String[] pre_data = line.split("	");
	               context.write(new Text("sort"), new Text(pre_data[0]));
	           
	          }
	         
	      }
	      
	   }

   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text word,  Iterable<Text > list,  Context context)
         throws IOException,  InterruptedException {
    	
    	  ArrayList<Text> cache = new ArrayList<Text>();
          for (Text aNum : list) {
        	  Text text = new Text();
              text.set(aNum.toString());
              cache.add(text);
          }
          int N = cache.size();						// all the input is stored in a array list and size of it gives number of lines in input file.	
          double rank = 1.0/N;
          for ( Text line  : cache) {				// 1/N gives inital rank
        	
        	 String title = "";			
        	 String links = "";
        	  Matcher title_boundry = Title_BOUNDARY.matcher(line.toString());
        	  while (title_boundry.find()) {
        		  title = (title_boundry.group(1));
   		          title = title + "####" + rank;		// finding title in each line with regex and appending  its present rank
   		       
   		    }
   		   Matcher body_boundry = Body_BOUNDARY.matcher(line.toString());
  		    while (body_boundry.find()) {
  		    											// got all the text in between the text tag using regex
  		        Matcher link_boundry = Link_BOUNDARY.matcher(body_boundry.group(1));
  			    while (link_boundry.find()) {
  			    	if(links.equals("")){
  			    	links = link_boundry.group(1);		//text between [[ ]] are found using regex from the text that is between text tag.
  			    	}else{
  			    		links = links+ "$@#$" + link_boundry.group(1);		//concating all the links separated by$@#$
  			    	}
  			        
  			    }
  		    	context.write(new Text(title), new Text(links));	//all are outputs are stored in format linl$@#$inlk2$@#$.....
         }
         }
        
      }
   }
   
   
   public static class Reduce_Process extends Reducer<Text ,  Text ,  Text ,  Text > {
	      @Override 
	      public void reduce( Text word,  Iterable<Text > list,  Context context)
	         throws IOException,  InterruptedException {
	    	   double rank=0 ;
	    	   String links="" ;
	    	  for (Text t : list){
	    		 String s = t.toString();			// if the text in Iterable has ####, then it is the line of page and their links
	    		  if(s.contains("####")){
	    			  String[] data = s.split("####");		//exceptions are handled 
	    			  										//otherwise add it to rank to get its next step rank
	    			  if(data.length<2){
	    				  links = "";
	    			  }
	    			  else{							//output is written in the exact format, how input is taken. so that next process job can handle it easily
	    			 links = data[1];
	    			  }
	    		  }
	    		  else{
	    			  rank = rank+ Double.valueOf(s);
	    		  }
	    	  }									//page rank formula and dapping factor takes care of dangling node.
	    	  rank = (1-damp_factor)+ damp_factor*(rank);
	    	  if(!links.equals("")){
	    		  context.write(new Text(word.toString()+"####"+String.valueOf(rank)), new Text(links));
	    	  }
	    	  
	      }
	   }
   
   public static class Reduce_Clean extends Reducer<Text ,  Text ,  Text ,  Text > {
	   
	   @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	      FileSystem fileSystem = FileSystem.get(context.getConfiguration());
	     
	        fileSystem.delete(new Path("intermediate"), true);// deletes the intermediate output files
	      super.cleanup(context);
	    }
	      @Override 
	      public void reduce( Text word,  Iterable<Text > list,  Context context)
	         throws IOException,  InterruptedException {			//collect all the page name and rank i a object and store in arraylist
	    	  ArrayList<sort> ObjectList = new ArrayList<sort>();
	    	  for (Text t : list){
	    		  ObjectList.add(new sort(t));
	    	  }														//sort objects with respect to their ranks
	    	  Collections.sort(ObjectList);
	    	  for(sort s : ObjectList){
	    		  context.write(new Text(s.name), new Text(String.valueOf(s.rank)));		//send the each page to output file and store in required format
	    	  }	
	      }
	   }
   
   public static class sort implements Comparable<sort> {
	   public String name;
	   public double rank;									//class for page object to sotre data and has implemented comparable to sort object by rank
	   sort(Text n){
		   this.name = n.toString().split("####")[0];
		   this.rank =Double.valueOf(n.toString().split("####")[1]);
	   }
	public int compareTo(sort o) {
		 if (this.rank<o.rank) return 1;
	        if (this.rank>o.rank) return -1;
	        return 0;
		
	}
	
   }

   
}






