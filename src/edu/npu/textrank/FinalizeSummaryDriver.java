package edu.npu.textrank;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Driver for finalizing summary sentence from output of Page Rank Job
 * @author Neeta Vekariya
 */
public class FinalizeSummaryDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
	// config a job and start it
	Configuration conf = getConf();
	Job job = new Job(conf, "Summarization");
	job.setJarByClass(FinalizeSummaryDriver.class);
	job.setMapperClass(FinalizeSummaryMapper.class);
	job.setReducerClass(FinalizeSummaryReducer.class);
	job.setNumReduceTasks(1);
	
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Text.class);
	job.setInputFormatClass(KeyValueTextInputFormat.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	FileSystem fs = FileSystem.get(conf);
	fs.delete(new Path(args[1]), true);
	int res = job.waitForCompletion(true) ? 0 : 1;
	return res;
    }

}
