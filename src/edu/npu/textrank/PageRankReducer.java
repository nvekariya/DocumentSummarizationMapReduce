package edu.npu.textrank;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for Pagerank Job
 * Do summation of ranks of given sentence and 
 * write down sentence and its record with new rank
 * @author Neeta Vekariya
 */
public class PageRankReducer extends
	Reducer<IntWritable, ObjectWritable, Text, SentenceRankRecord> {

    protected void reduce(
    		IntWritable key,
	    java.lang.Iterable<ObjectWritable> val,
	    org.apache.hadoop.mapreduce.Reducer<IntWritable, ObjectWritable, Text, SentenceRankRecord>.Context context)
	    throws java.io.IOException, InterruptedException {
	double rank = .0;
	SentenceRankRecord record = new SentenceRankRecord();
	for (ObjectWritable w : val) {
	    if (w.getDeclaredClass().toString().equals(
		    DoubleWritable.class.toString())) {
		rank += ((DoubleWritable) w.get()).get();
	    }
	    if (w.getDeclaredClass().toString().equals(
	    		SentenceRankRecord.class.toString())) {
	    	record = (SentenceRankRecord) w.get();
	    }
	}
	
	context.write(new Text(String.valueOf(key.get())), new SentenceRankRecord(record.getSentence(),  rank, record.getsentenceSimilarityMap()));
    };
}
