package edu.npu.textrank;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for Pagerank Job
 * it retrieves all sentences and its rank, 
 * finalize one sentence with highest rank
 * and writes it as output
 * @author Neeta Vekariya
 */
public class FinalizeSummaryReducer extends
	Reducer<NullWritable, Text, NullWritable, Text> {

    protected void reduce(
    		NullWritable key,
	    java.lang.Iterable<Text> val,
	    org.apache.hadoop.mapreduce.Reducer<NullWritable, Text, NullWritable, Text>.Context context)
	    throws java.io.IOException, InterruptedException {
	double rank = .0;
	String sentence=null;
	String[] text;
	for (Text t : val) {
	    text = t.toString().split("<RANK>");
	    double r = 0.0;
	    if(text.length == 2){
	    	r = Double.parseDouble(text[1]);
	    }
	    if(r > rank){
	    	rank = r;
	    	sentence = text[0];
	    }
	}
	
	context.write(NullWritable.get(), new Text(sentence));
    };
}
