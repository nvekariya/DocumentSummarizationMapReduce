package edu.npu.textrank;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;


/**
 * Driver that summarize the document
 * @author Neeta Vekariya
 *
 */
public class TextRank {

	private final static Log LOG =
	        LogFactory.getLog(TextRank.class.getName());
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		LOG.info("Start Text Rank Algorithm for document summarization");
		Configuration conf = new Configuration();
		
		conf.set("textinputformat.record.delimiter", ".");
		LOG.info("File Separator delimiter: "+conf.get("textinputformat.record.delimiter"));
	    
		/**
		 * Start Text Rank job to get normalized matrix
		 */
		
		// Add file of Stop words into distributed cache 
		// that is used to filter stop words while calculating text rank algorithm
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);
		
    	
		Job job = new Job(conf, "TextRank");
	    job.setJarByClass(TextRank.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	        
	    job.setMapOutputKeyClass(NullWritable.class);
	    job.setMapOutputValueClass(SortedMapWritable.class);
	    
	    job.setMapperClass(TextRankMapper.class);
	    job.setReducerClass(TextRankReducer.class);
	    job.setNumReduceTasks(1);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	       
	    
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[2]), true);
		
		
	    if(job.waitForCompletion(true)){
	    	/**
			 * convert normalized matrix into graph
			 * where node represents sentence and edges between nodes represents 
			 * similarity (content overlap) between those sentences
			 * Then run Page Rank Job to calculate importance of each sentence
			 */
	    	String[] pageRankArgs = new String[]{args[2], args[3]};
		    int res = 0;
		    int iterations = 15;
		    if(null != args[4]){
		    	iterations = Integer.parseInt(args[4]);
		    }
		    LOG.info("Start Page Rank Algo");
			
			for (int i = 0; i < iterations; i++) {
				LOG.info("Page Rank Iteration: " + i);
			    res = ToolRunner.run(new Configuration(), new PageRankDriver(),
			    		pageRankArgs);
			}
			if(res == 0){
				/**
				 * Run final job to find node (sentence) with highest importance 
				 */
				LOG.info("Run Summarization job to get summary sentence");
				res = ToolRunner.run(new Configuration(), new FinalizeSummaryDriver(),
			    		pageRankArgs);
			}else{
				LOG.error("Error while iterating through Page Job");
			}
	    }else{
	    	LOG.error("Error while running Text Rank Job");
		}
	}

}
