package heigvd.bda.labs.redditanalytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Word Count example of MapReduce job.
 *
 * Given a plain text as input on local or HDFS, this job counts how many occurrences of
 * each word in that text and writes the result on local or HDFS.
 *
 * The input can be a single text file or a folder containing multiple text files.
 *
 * usage: WordCount <num_reducers> <input_path> <output_path>
 *
 * @author fatemeh.borran
 *
 */
public class RedditAnalytics extends Configured implements Tool {

	public final static IntWritable ONE = new IntWritable(1);

	private int numReducers;
	private Path inputPath;
	private Path outputPath;

	/**
	 * WordCount Constructor.
	 *
	 * @param args
	 */
	public RedditAnalytics(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: WordCount <num_reducers> <input_path> <output_path>");
			System.exit(0);
		}
		numReducers = Integer.parseInt(args[0]);
		inputPath = new Path(args[1]);
		outputPath = new Path(args[2]);
	}

	/**
	 * Utility to split a line of text in words.
	 *
	 * @param text what we want to split
	 * @return words in text as an Array of String
	 */
	public static String[] words(String text) {
	    StringTokenizer st = new StringTokenizer(text);
	    ArrayList<String> result = new ArrayList<String>();
	    while (st.hasMoreTokens())
	    	result.add(st.nextToken());
	    return Arrays.copyOf(result.toArray(),result.size(),String[].class);
	}


	/**
	 * Simple Mapper class for WordCount
	 *
	 * Input: (LongWritable id, Text line)
	 * Output: (Text word, IntWritable 1)
	 *
	 * @author fatemeh.borran
	 *
	 */
	static class PostJoinMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text text;
		private Submission submission;
		/**
		 * The setup before map.
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			text = new Text();
			submission = new Submission();
		}

		/**
		 * The map method reads an id as key and a text as value
		 * and emits the pair (word,1) using Mapper.context.write()
		 *
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {


				submission.setJson(value.toString());
				text.set(submission.getId());
				context.write(text, RedditAnalytics.ONE);

		}

		/**
		 * The cleanup after map.
		 */
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			super.cleanup(context);
		}

	}

	/**
	 * Reducer class for WordCount sums results for a given word.
	 *
	 * Input: (Text word, IntWritable 1)
	 * Output: (Text word, IntWritable sum)
	 *
	 * @author fatemeh.borran
	 *
	 */
	static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable res = new IntWritable();

		/**
		 * The setup before reduce.
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			res = new IntWritable();
		}

		/**
		 * The reduce method reads an id as key and an iterable collection of 1 as values
		 * and emits the pair (word,sum) using Reducer.context.write()
		 */
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable value : values)
				sum += value.get();

			res.set(sum);
			context.write(key, res);
		}

		/**
		 * The cleanup after reduce.
		 */
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			super.cleanup(context);
		}
	}

	/**
	 * The main method to define the job and run the job.
	 */
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		// Create a new Job
		Job job = new Job(conf,"Word Count Simple");

		// Set job input format to Text:
		// Files are broken into lines.
		// Either linefeed or carriage-return are used to signal end of line.
		// Keys are the position in the file, and values are the line of text.
		job.setInputFormatClass(TextInputFormat.class);

		// Set map class and the map output key and value classes
		job.setMapperClass(PostJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Set reduce class and the reduce output key and value classes
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set job output format to Text
		job.setOutputFormatClass(TextOutputFormat.class);

		// Add the input file as job input (from local or HDFS) to the variable inputPath
		FileInputFormat.addInputPath(job, inputPath);

		// Set the output path for the job results (to local or HDFS) to the variable outputPath
		FileOutputFormat.setOutputPath(job, outputPath);

		// Set the number of reducers using variable numReducers
		job.setNumReduceTasks(numReducers);

		// Set the jar class
		job.setJarByClass(RedditAnalytics.class);

		// Execute the job
		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAnalytics(args), args);
		System.exit(res);
	}
}
