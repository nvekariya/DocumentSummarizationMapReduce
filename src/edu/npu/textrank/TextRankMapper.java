package edu.npu.textrank;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper to generate Bag of words for each sentence
 * @author Neeta Vekariya
 */
public class TextRankMapper extends Mapper<LongWritable, Text, NullWritable, SortedMapWritable> {
	
	private final static Log LOG =
	        LogFactory.getLog(TextRankMapper.class.getName());
	HashSet<String> stopWords = new HashSet<String>();
	/**
	 * Read stop words from distributed cache
	 */
	public void setup(Context context) throws IOException, InterruptedException{

	    Configuration conf = context.getConfiguration();
	    Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
	    String file = localFiles[0].toString();
	    //String file = "/home/cs570/Desktop/TextRankInput/en-stopwords.txt";
	    BufferedReader in = new BufferedReader(new InputStreamReader( new FileInputStream(new File(file))));
	    while (in.ready()) {  			
			stopWords.add(in.readLine().trim().toLowerCase());
		}
	    LOG.info("Stop words count:"+stopWords.size());
	    in.close();
	}
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	SortedMapWritable map = new SortedMapWritable();
    	String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        String token;
        Text textToken;
        // Generate Map, representing Bag Of Wrods, for each sentence 
        while (tokenizer.hasMoreTokens()) {
        	token = tokenizer.nextToken().toLowerCase().replace(".", "").replace(",", "");
        	if(!stopWords.contains(token)){
        		textToken = new Text(token);
            	IntWritable count = (IntWritable) map.get(textToken);
				if (count != null) { 
					count.set(count.get() + 1);
				}else{
					map.put(textToken, new IntWritable(1));
				}
        	}
        }
        SortedMapWritable newMap = new SortedMapWritable();
        newMap.put(value, map);
       context.write(NullWritable.get(), newMap);
        
    }
 } 
