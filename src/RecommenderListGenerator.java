import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RecommenderListGenerator {

	public RecommenderListGenerator() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("coOcuurencePath", args[0]);
		conf.set("watchHistory", args[1]);
		conf.set("movieTitle", args[2]);
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(RecommenderListGeneratorMapper.class);
		job.setReducerClass(RecommenderListGeneratorReducer.class);
		
		job.setJarByClass(RecommenderListGenerator.class);
		
		job.setNumReduceTasks(3);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		//Since the mapper output and reducer input is not same
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

	public static class RecommenderListGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		
		
		Map<Integer, List<MovieRelation>> movieRelationMap = new HashMap<>();
		Map<Integer, Integer> denominator = new HashMap<>();
		Map<Integer, List<Integer>> watchHistory = new HashMap<>();
		
		/**
		 * [movie1 {movie1, movie2, 8} {movie1, movie3, 5}{movie1,movie7,6}]
		 * [movie2 {movie2, movie1, 8} {movie2, movie3, 5}{movie2,movie7,6}
		 */
		/**
		 * read movie watch history for simulation
		 * actually, the watch history should be stored in the database rather than stay in the file
		 */
		@Override
		protected void setup(Context context) throws IOException{
			//<movie_id, movie_title>
			//read movie title from file
			Configuration conf = context.getConfiguration();
			String filePath = conf.get("watchHistory");
			Path pt = new Path(filePath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			//input: user, movie, rating
			while (line != null){
				
				int user = Integer.parseInt(line.split(",")[0]);
				int movie =Integer.parseInt(line.split(",")[1]);
				if (watchHistory.containsKey(user)){
					watchHistory.get(user).add(movie);			
				} else {
					List<Integer> newList = new ArrayList<>();
					newList.add(movie);
					watchHistory.put(user, newList);
				}
				line = br.readLine();
			}

			br.close();
			
		}
		@Override
		public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException{
			
			//Reading in the Rating Matrix
			//input user, movie,rationg
			//output user:movie score
			String[] tokens = value.toString().trim().split(",");
			int user = Integer.parseInt(tokens[1]);
			int movie = Integer.parseInt(tokens[1]);
			
			if (!watchHistory.get(user).contains(movie)){
				context.write(new IntWritable(user), new Text(movie + ":" +tokens[2]));
			}
			
		}
	}
	
	public static class RecommenderListGeneratorReducer extends Reducer< Text, DoubleWritable, IntWritable, Text>{
		
		Map<Integer, String> movieTitles = new HashMap<>();
		
		protected void setup(Context context) throws IOException{
			//<movie_id, movie_title>
			//read movie title from file
			
			//<movie_id, movie_title>
			//read movie title from file
			Configuration conf = context.getConfiguration();
			String filePath = conf.get("movieTitles");
			Path pt = new Path(filePath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			//input: user, movie, rating
			while (line != null){
				
				int movieid = Integer.parseInt(line.split(",")[0]);
				String movieTitle =line.split(",")[1];
				if (movieTitles.containsKey(movieid)){
					movieTitles.get(movieid).add(movieTitle);			
				} else {
					movieTitles.put(movieid, movieTitle);
				}
				line = br.readLine();
			}

			br.close();
			
		
		}
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException{
			/**
			*user:movie score
			*/
			//key movie1:movie2 value = iterable<1,1,1>
			double sum = 0;
			while (values.iterator().hasNext()){
				sum += values.iterator().next().get();
			}
			
			String[] tokens = key.toString().trim().split(":");
			int user = Integer.parseInt(tokens[0]);
			context.write(new IntWritable(user),  new Text(tokens[1] + ":" +sum));
		}
	}
}
