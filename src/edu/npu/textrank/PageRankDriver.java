package edu.npu.textrank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Driver for Pagerank Job
 * @author Neeta Vekariya
 */
public class PageRankDriver extends Configured implements Tool {

	
    @Override
    public int run(String[] args) throws Exception {
	// config a job and start it
	Configuration conf = getConf();
	Job job = new Job(conf, "Rank");
	job.setJarByClass(PageRankDriver.class);
	job.setMapperClass(PageRankMapper.class);
	job.setReducerClass(PageRankReducer.class);
	
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(ObjectWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(ObjectWritable.class);
	job.setInputFormatClass(KeyValueTextInputFormat.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	FileSystem fs = FileSystem.get(conf);
	fs.delete(new Path(args[1]), true);
	int res = job.waitForCompletion(true) ? 0 : 1;
	if (res == 0) {
		fs.delete(new Path(args[0]), true);
	    fs.rename(new Path(args[1]), new Path(args[0]));
	}
	return res;
    }

}
