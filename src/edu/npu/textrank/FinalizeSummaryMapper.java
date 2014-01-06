package edu.npu.textrank;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper 
 * It take page rank output as input and writes sentence and its rank as intermediate output
 * @author Neeta Vekariya
 * 
 */
public class FinalizeSummaryMapper extends Mapper<Text, Text, NullWritable, Text> {
    protected void map(
    		Text key,
	    Text value,
	    org.apache.hadoop.mapreduce.Mapper<Text, Text, NullWritable, Text>.Context context)
	    throws java.io.IOException, InterruptedException {
	SentenceRankRecord record = new SentenceRankRecord(value.toString());
	
	// output sentence and its rank
	context.write(NullWritable.get(), new Text(record.getSentence()+"<RANK>"+record.getRank()));
    };
}
