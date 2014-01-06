package edu.npu.textrank;


import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for Pagerank Job
 * Calculate rank for each outlink sentence and write it down
 * write down given senence and its record
 * @author Neeta Vekariya
 * 
 */
public class PageRankMapper extends Mapper<Text, Text, IntWritable, ObjectWritable> {
    protected void map(
    		Text key,
	    Text value,
	    org.apache.hadoop.mapreduce.Mapper<Text, Text, IntWritable, ObjectWritable>.Context context)
	    throws java.io.IOException, InterruptedException {
	// read information from line
	IntWritable sentence = new IntWritable(Integer.parseInt(key.toString()));
	SentenceRankRecord record = new SentenceRankRecord(value.toString());
	Set<Entry<Integer, Double>> set = record.getsentenceSimilarityMap().entrySet();
	Iterator<Entry<Integer, Double>> itr = set.iterator();
    while(itr.hasNext()){
    	Entry<Integer,Double> entry = itr.next();
       // output sentence rank
	    double newRank = record.getRank() * entry.getValue();
	    context.write(new IntWritable(entry.getKey()), new ObjectWritable(new DoubleWritable(
		    newRank)));
	}

	// output sentence list
	context.write(sentence, new ObjectWritable(record));
    };
}
