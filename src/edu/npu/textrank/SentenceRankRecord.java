package edu.npu.textrank;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * PageRank Record Class 
 * storing Sentence, its rank, and sentence list (hashcode)  with which given sentence has content overlap
 * 
 * @author Neeta Vekariya
 * 
 */
public class SentenceRankRecord implements Writable {
	private String sentence;
    private double rank;
    private HashMap<Integer, Double> sentenceSimilarityMap = new HashMap<Integer, Double>();

    public SentenceRankRecord() {
    }

	public double getRank() {
	return rank;
    }

    

    public String getSentence() {
		return sentence;
	}

	public HashMap<Integer, Double> getsentenceSimilarityMap() {
		return sentenceSimilarityMap;
	}

	public SentenceRankRecord(String str) {
		super();
		String[] items = str.split("<sentence-rank>");
		this.sentence = items[0];
		this.rank = Double.parseDouble(items[1]);
		if (items.length == 3){
			converSentenceSimilarityStringToMap(items[2]);
		}
    }

    public SentenceRankRecord(String sentence, double rank, HashMap<Integer, Double> sentenceSimilarityMap) {
	super();
	this.sentence = sentence;
	this.rank = rank;
	this.sentenceSimilarityMap = sentenceSimilarityMap;
    }

    private void converSentenceSimilarityStringToMap(String str){
    	String[] sentenceSimilarities = str.split("<;>");
	    for(String ss: sentenceSimilarities){
	    	String[] strs = ss.split("<=>");
	    	if(strs.length == 2){
	    		sentenceSimilarityMap.put(Integer.parseInt(strs[0]), Double.parseDouble(strs[1]));
	    	}
	    }
    }
    
    @Override
	public String toString() {
		StringBuilder sb = new StringBuilder(sentence+"<sentence-rank>"+rank+"<sentence-rank>");
		sb.append(converSentenceSimilarityMapToString());
		return sb.toString();
	}

	private String converSentenceSimilarityMapToString(){
    	StringBuilder sb = new StringBuilder();
    	Set<Entry<Integer, Double>> set = sentenceSimilarityMap.entrySet();
    	Iterator<Entry<Integer, Double>> itr = set.iterator();
	    while(itr.hasNext()){
	    	Entry<Integer,Double> entry = itr.next();
	    	if(sb.length() > 0){
	    		sb.append("<;>"+entry.getKey()+"<=>"+entry.getValue());
	    	}else{
	    		sb.append(entry.getKey()+"<=>"+entry.getValue());
	    	}
	    }
	    return sb.toString();
    }
    @Override
    public void readFields(DataInput in) throws IOException {
    	Text sentence = new Text();
	DoubleWritable dw = new DoubleWritable();
	Text aw = new Text();
	sentence.readFields(in);
	this.sentence = sentence.toString();
	dw.readFields(in);
	aw.readFields(in);
	rank = dw.get();
	converSentenceSimilarityStringToMap(aw.toString());
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	Text sent = new Text(sentence);
	DoubleWritable dw = new DoubleWritable(rank);
	Text aw = new Text(converSentenceSimilarityMapToString());
	sent.write(out);
	dw.write(out);
	aw.write(out);
    }
}
