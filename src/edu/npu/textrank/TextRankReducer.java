package edu.npu.textrank;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer that retrieves Bag of words for each sentence
 * and calculate tf-idf score for each word in each sentence
 * and create normalized matrix 
 * where columns and rows are sentences and value 
 * represents similarity (content overlap) between those sentence
 * @author Neeta Vekariya
 */
public class TextRankReducer extends Reducer<NullWritable, SortedMapWritable, Text, SentenceRankRecord> {

	//Hashmap that stores unique words found in document and their sentence count in document
	private Map<String, Integer> uniqueWords = new HashMap<String, Integer>();
	
	// sentences stores all sentences in document
	private List<String> sentences = new ArrayList<String>();
	
	// list of maps that stores bag of words for all sentences
	private List<SortedMapWritable> sentencesMap = new ArrayList<SortedMapWritable>();
	
	// list that stores maximum frequency of any word, in a sentence, for each sentence. 
	//it is used to calculate term frequency of all words in that sentence
	private List<Integer> maxFrequencyOfAnyWordInASentense = new ArrayList<Integer>();
	
	/**
	 * Create hashmap: uniqueWord that stores unique words found in document and their sentence count in document
	 */
    public void reduce(NullWritable key, Iterable<SortedMapWritable> values, Context context) 
      throws IOException, InterruptedException {
    	 
    	 for (SortedMapWritable val : values) {
        	SortedMapWritable map;
        	String word;
        	int maxFreq=0, count=0;
        	for(Entry<WritableComparable, Writable> entry : val.entrySet()){
            	if(sentences.contains(entry.getKey().toString())) continue;
            	sentences.add(entry.getKey().toString());
            	map = (SortedMapWritable)entry.getValue();
            	sentencesMap.add(map);
            	maxFreq=0;
            	for(Entry<WritableComparable, Writable> wordCountEntry : map.entrySet()){
            		word = wordCountEntry.getKey().toString();
            		count = ((IntWritable)wordCountEntry.getValue()).get();
            		int sentenceCount = 0;
            		if(uniqueWords.get(word) != null){
            			sentenceCount = uniqueWords.get(word);
            		}
					uniqueWords.put(word, sentenceCount+1);
					
					if(maxFreq < count){
						maxFreq = count;
	        		}
            	}
            	maxFrequencyOfAnyWordInASentense.add(maxFreq);
            }
           
        }
    	
    }
	
    /**
     * Create normalized matrix and write data as input to the page rank job
     */
    protected void cleanup(Context context)
			throws IOException, InterruptedException {
    	int noOfSentences = sentences.size();
		int uniqWords = uniqueWords.size();
		
		/**
	     * Create Sentence word count matrix
	     * where rows are sentences and columns are all unique words in a document
	     * and value represents frequency of that word (term) in that sentence
	     */
    	int[][] matrix = new int[noOfSentences][uniqWords];
    	SortedMapWritable map;
        Integer count;
		for(int i=0; i<noOfSentences; i++){
			map = sentencesMap.get(i);
			int j=0;
			IntWritable intCnt;
	        for(String uniqWord: uniqueWords.keySet()){
	        	intCnt = (IntWritable)map.get(new Text(uniqWord));
	        	if(intCnt == null){
	        		count = 0;
	        	}else{
	        		count = (intCnt).get();
		        }
	        	matrix[i][j] = count;
	        	j++;
	        }
		}
		
		
        double[][] sentenceScoreMatrix = getSentenceScoreMatrix(matrix);
		
		double[][] transposeSentenceScoreMatrix = getTransposeMatrix(sentenceScoreMatrix);
		
		double[][] mirrorMatrix = getMirroredMatrix(sentenceScoreMatrix, transposeSentenceScoreMatrix);
		
		writeOutput(mirrorMatrix, context);
	}
    
    /**
     * Write output that will be input to page rank job
     * @param mrrMtrx
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void writeOutput(double[][] mrrMtrx, Context context) throws IOException, InterruptedException{
    	int noOfSentences = sentences.size();
		double initRank = 1.0/noOfSentences;
		for(int row = 0; row < mrrMtrx.length; row++){
			int key = sentences.get(row).hashCode();
			HashMap<Integer, Double> sentenceSimilarityMap = new HashMap<Integer, Double>();
			for(int col = 0; col < mrrMtrx[row].length;col++){
				if(mrrMtrx[row][col] != 0.0){
					sentenceSimilarityMap.put(sentences.get(col).hashCode(), mrrMtrx[row][col]);
				}
			}
			// key : sentence (hashcode)
			// value : sentence, its rank and 
			// map that stores sentences, with which given sentence has similarity, as key and similarity as value
			context.write(new Text(String.valueOf(key)), new SentenceRankRecord(sentences.get(row), initRank, sentenceSimilarityMap));
		}
    }
    
    /**
     * calculate tf, idf score for each word in a sentence
     * tf (Term Frequency) = frequency of given word / max frequency of any word in that sentence
     * idf (Inverse Document Frequency) = Log2(total number of sentences / number of sentences in which that word exists).
     * @param matrixSentencesWordCount
     * @return
     */
	private double[][] getSentenceScoreMatrix(int[][] matrixSentencesWordCount){
		int noOfSentences = sentences.size();
		int uniqWordsCnt = uniqueWords.size();
		
		double[][] scoreMatrix = new double[noOfSentences][uniqWordsCnt];
		
		int j, sentenceCount;
		double idf,tf, tfidf;
		for(int i=0; i<noOfSentences; i++){
			
			j=0;
			for(String uniqWord: uniqueWords.keySet()){
				sentenceCount = uniqueWords.get(uniqWord);
				idf = Math.log(noOfSentences/(double)sentenceCount) / Math.log(2);
				
				tf =  (matrixSentencesWordCount[i][j]) / (double)maxFrequencyOfAnyWordInASentense.get(i);
				tfidf = tf * idf;
				scoreMatrix[i][j] = tfidf;
				j++;
			}
		}
		return scoreMatrix;
	}
	
	/**
	 * Create Transpose matrix of given score matrix
	 * @param matrix
	 * @return
	 */
	private double[][] getTransposeMatrix(double[][] matrix){
		int noOfSentences = sentences.size();
		int uniqWordsCnt = uniqueWords.size();
		
		double[][] transposeMatrix = new double[uniqWordsCnt][noOfSentences];
		
		for(int i=0; i<noOfSentences; i++){
			for(int j=0; j<uniqWordsCnt; j++){
				transposeMatrix[j][i] = matrix[i][j];
			}
		}
		
		return transposeMatrix;
	}
	
	/**
	 * Normalize given score matrix and transpose matrix by using Cosine Similarity equation
	 * and generate mirror matrix where rows and columns are sentences 
	 * and value represents similarity (content overlap) between those sentences
	 * @param sentenceScoreMatrix
	 * @param transposeSentenceScoreMatrix
	 * @return
	 */
	private double[][] getMirroredMatrix(double[][] sentenceScoreMatrix, double[][] transposeSentenceScoreMatrix){
        int noOfSentences = sentences.size();
        int uniqWordsCnt = uniqueWords.size();
        
        double[][] mirrorMatrix = new double[noOfSentences][noOfSentences];
        
        double similarityScore;
        double similarityScoreNom;
        double similarityScoreDenom, similarityScoreDenomA, similarityScoreDenomB,a,b;
        for(int i1=0; i1<noOfSentences; i1++){
            for(int i2=0; i2<noOfSentences; i2++){
                similarityScore = 0.0;
                similarityScoreNom = 0.0;
                similarityScoreDenomA = 0.0;
                similarityScoreDenomB = 0.0;
                
                for(int j=0; j<uniqWordsCnt; j++){
                    a = sentenceScoreMatrix[i1][j];
                    b = transposeSentenceScoreMatrix[j][i2];
                        similarityScoreNom += a*b;
                        similarityScoreDenomA += a*a;
                        similarityScoreDenomB += b*b;
                        
                }
                similarityScoreDenom  = Math.sqrt(similarityScoreDenomA) * Math.sqrt(similarityScoreDenomB);
                similarityScore = similarityScoreNom / similarityScoreDenom;
                mirrorMatrix[i1][i2] = similarityScore;
            }
            
        }
         return mirrorMatrix;
    }

 }
