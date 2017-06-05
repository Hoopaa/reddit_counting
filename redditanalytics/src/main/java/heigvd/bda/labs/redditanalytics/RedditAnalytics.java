package heigvd.bda.labs.redditanalytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Reddit Analytics : 
 * first step : join post and comment
 * 
 *  second step : extract some data of the full json and make some analytics like sum, avg ...
 *
 * usage: redditanalytics <num_reducers> <input_path_comments> <input_path_posts> <output_path>
 *
 * @author Magnin - Pasquier - Pfeiffer - van Dooren
 *
 */
public class RedditAnalytics extends Configured implements Tool {


	private int numReducers;
	private Path inputPath1;
	private Path inputPath2;
	private Path outputPath;
	private Path tmpPath;
	

	/**
	 * RedditAnalytics Constructor.
	 *
	 * @param args
	 */
	public RedditAnalytics(String[] args) {
		if (args.length != 4) {
			System.out.println("Usage: WordCount <num_reducers> <input_path_comments> <input_path_post> <output_path>");
			System.exit(0);
		}
		numReducers = Integer.parseInt(args[0]);
		inputPath1 = new Path(args[1]);
		inputPath2 = new Path(args[2]);
		outputPath = new Path(args[3]);
		tmpPath = new Path("tmp");
	}

	/**
	 * Simple Mapper class for PostJoin
	 *
	 * Input: (Object id, Text line (json post))
	 * Output: (Text post ID, Text "A" + (json post))
	 *
	 * @author Magnin - Pasquier - Pfeiffer - van Dooren
	 *
	 */ 
	static class PostJoinMapper extends Mapper<Object, Text, Text, Text> {

		private Text text, body;
		private Submission submission;
		/**
		 * The setup before map.
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			super.setup(context);
			text = new Text();
			body = new Text();
			submission = new Submission();
		}

		/**
		 *  le contenu du json du post est prefixe de "A" pour signifier qu'il s'agit d'un post
		 *  la cle est l'ID du post
		 */
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

				submission.setJson(value.toString());
				text.set(submission.getId());
				body.set("A" + submission.getAll().replaceAll("\n", "\t"));
				context.write(text, body);
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
	 * Simple Mapper class for CommentJoin
	 *
	 * Input: (Object id, Text line (json comment))
	 * Output: (Text post ID, Text "B" + (json comment))
	 *
	 * @author Magnin - Pasquier - Pfeiffer - van Dooren
	 *
	 */ 
	static class CommentJoinMapper extends Mapper<Object, Text, Text, Text> {

		private Text text, body;
		private Comment comment;
		/**
		 * The setup before map.
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			super.setup(context);
			text = new Text();
			body = new Text();
			comment = new Comment();
		}

		/**
		 *  le contenu du json du commentaire est prefixe de "B" pour signifier qu'il s'agit d'un commentaire
		 *  la cle est l'ID du post du commentaire
		 */
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

				comment.setJson(value.toString());
				text.set(comment.getLinkId().split("_")[1]);
				body.set("B" + comment.getAll().replaceAll("\n", "\t"));
				context.write(text, body);
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
	 * Simple Reducer class for Join between post and comment (by comment ID) 
	 *
	 * Input: (Text post id, Text (json comment) or (json post))
	 * Output: (Text json contain post with comment, Text Empty)
	 *
	 * @author Magnin - Pasquier - Pfeiffer - van Dooren
	 *
	 */ 
	static class WCReducer extends Reducer<Text, Text, Text, Text> {

		private IntWritable res = new IntWritable();
		private ArrayList<Text> LstA;
		private ArrayList<Text> LstB;
		private Text tmp;
		private JSONObject jsonA, jsonB;
		private Text emptyText;

		/**
		 * The setup before reduce.
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			super.setup(context);
			res = new IntWritable();
			LstA = new ArrayList<>();
			LstB = new ArrayList<>();
			tmp = new Text();	
			emptyText = new Text("");
			
		}

		/**
		 * The reduce method join comment and post by post id
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			LstA.clear();
			LstB.clear();
			
		
			for (Text value : values)
			{
				tmp.set(value);
				if(tmp.charAt(0) == 'A')
				{
					LstA.add(new Text(tmp.toString().substring(1)));
				}
				else if(tmp.charAt(0) == 'B')
				{
					LstB.add(new Text(tmp.toString().substring(1)));
				}
			}
			if (!LstA.isEmpty() && !LstB.isEmpty()) 
			{
				for (Text A : LstA) 
				{
					for (Text B : LstB) {
						
						try {
							jsonA = new JSONObject(A.toString());
							jsonB = new JSONObject(B.toString());
							jsonA.put("comment", jsonB);
							
							tmp.set(jsonA.toString());
							context.write(tmp, emptyText);
							
						} catch (JSONException e) {
							e.printStackTrace();
						}

					}
				}
			}
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
	 * Simple Mapper class for extract some data from the json 
	 *
	 * Input: (Object id, Text (json post) and (json comment))
	 * Output: (Text key composit contain some info of post, Text value composit contain some info of comment)
	 *
	 * @author Magnin - Pasquier - Pfeiffer - van Dooren
	 *
	 */ 
	static class AnalyticsMapper extends Mapper<Object, Text, Text, Text> {

		private Text post, comment;
		private Analytic analytics;
		private String compositeKey, compositeValue;
		/**
		 * The setup before map.
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			post = new Text();
			comment = new Text();
			analytics = new Analytic();
		}

		/**
		 * le mapper extrait certaine donnees du post et du commentaire 
		 * la cle composite contient les infos relatives au post et la valeur 
		 */
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
				
				analytics.setJson(value.toString());
				compositeKey = analytics.getSubmission().getId() + '\n' + analytics.getSubmission().getScore() + '\n' + analytics.getSubmission().getNumComments() + '\n' + analytics.getSubmission().getBody().length();
				compositeValue = analytics.getComment().getScore() + '\n' + analytics.getComment().getBody().length();				
				post.set(compositeKey);
				comment.set(compositeValue);
				context.write(post, comment);
				
				
				System.out.println(analytics.getSubmission().getBody().length());
				
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
	 * Simple Reducer class for extract some data from the json 
	 *
	 * Input: (Text composit key of post, Text composit value of comment)
	 * Output: (Text post id + some values for analytics, Text Empty)
	 *
	 * @author Magnin - Pasquier - Pfeiffer - van Dooren
	 *
	 */ 
	static class AnalyticsReducer extends Reducer<Text, Text, Text, Text> {

		private DoubleWritable avgLengthComment = new DoubleWritable();
		private DoubleWritable avgScore = new DoubleWritable();
		private ArrayList<String> tabPost;
		private ArrayList<String> tabComment;
		private Text key, value;

		/**
		 * The setup before reduce.
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			tabPost = new ArrayList<>();
			tabComment = new ArrayList<>();
			key = new Text();
			value = new Text();
			
			
		}

		/**
		 * Le reducer effectue principalement des moyenes et ecrit en fichier de sortie les valeurs separee par des ";"
		 */
		@Override
		protected void reduce(Text post, Iterable<Text> comments, Context context)
				throws IOException, InterruptedException {
			tabPost.clear();
			tabPost.addAll(Arrays.asList(post.toString().split("\n")));
			avgLengthComment.set(0);
			avgScore.set(0);;
			for (Text comment : comments){
				tabComment.clear();
				tabComment.addAll(Arrays.asList(comment.toString().split("\n")));
				
				avgLengthComment.set(avgLengthComment.get() + Double.valueOf(tabComment.get(1)));
				avgScore.set(avgScore.get() + Double.valueOf(tabComment.get(0)));
				
			}
			if(Integer.valueOf(tabPost.get(2))!=0)
			{
				avgLengthComment.set(avgLengthComment.get() / Double.valueOf(tabPost.get(2)));
				avgScore.set(avgScore.get() / Double.valueOf(tabPost.get(2)));
			}
			else
			{
				avgLengthComment.set(0);
				avgScore.set(0);
			}
			key.set(tabPost.get(0)+";"+tabPost.get(1)+";"+tabPost.get(3)+";"+avgLengthComment.get()+";"+avgScore.get()+";"); 
			context.write(key, value);
			
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
		Job job = new Job(conf,"Join Reddit Analytics");

		// Set job input format to Text:
		// Files are broken into lines.
		// Either linefeed or carriage-return are used to signal end of line.
		// Keys are the position in the file, and values are the line of text.
		job.setInputFormatClass(TextInputFormat.class);

		// Set map class and the map output key and value classes
		MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class,CommentJoinMapper.class);
		MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class,PostJoinMapper.class);

		// Set reduce class and the reduce output key and value classes
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set job output format to Text
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the output path for the job results (to local or HDFS) to the variable outputPath
		FileOutputFormat.setOutputPath(job, tmpPath);

		// Set the number of reducers using variable numReducers
		job.setNumReduceTasks(numReducers);

		// Set the jar class
		job.setJarByClass(RedditAnalytics.class);
		
		// Execute the job
		job.waitForCompletion(true);
		
		// Create a new Job
		Job jobAnalytic = new Job(conf,"Reddit Analytics Stats");

		// Set job input format to Text:
		// Files are broken into lines.
		// Either linefeed or carriage-return are used to signal end of line.
		// Keys are the position in the file, and values are the line of text.
		jobAnalytic.setInputFormatClass(TextInputFormat.class);

		// Set map class and the map output key and value classes
		jobAnalytic.setMapperClass(AnalyticsMapper.class);
		jobAnalytic.setMapOutputKeyClass(Text.class);
		jobAnalytic.setMapOutputValueClass(Text.class);

		// Set reduce class and the reduce output key and value classes
		jobAnalytic.setReducerClass(AnalyticsReducer.class);
		jobAnalytic.setOutputKeyClass(Text.class);
		jobAnalytic.setOutputValueClass(Text.class);

		// Set job output format to Text
		jobAnalytic.setOutputFormatClass(TextOutputFormat.class);

		// Add the input file as job input (from local or HDFS) to the variable inputPath
		FileInputFormat.addInputPath(jobAnalytic, tmpPath);

		// Set the output path for the job results (to local or HDFS) to the variable outputPath
		FileOutputFormat.setOutputPath(jobAnalytic, outputPath);

		// Set the number of reducers using variable numReducers
		jobAnalytic.setNumReduceTasks(numReducers);

		// Set the jar class
		jobAnalytic.setJarByClass(RedditAnalytics.class);

		// Execute the job
		int res = jobAnalytic.waitForCompletion(true) ? 0 : 1;
		//FileSystem.delete(tmpPath, true);
		return res;
	}


	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAnalytics(args), args);
		System.exit(res);
	}
}