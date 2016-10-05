import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CoOccurenceMatrixGenerator {

	public CoOccurenceMatrixGenerator() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(CoOccurenceMatrixMapper.class);
		job.setReducerClass(CoOccurenceMatrixReducer.class);
		
		job.setJarByClass(CoOccurenceMatrixGenerator.class);
		
		job.setNumReduceTasks(3);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

	public static class CoOccurenceMatrixMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException{
			/**
			String[] user_movie_rating = value.toString().trim().split(",");
			int userID = Integer.parseInt(user_movie_rating[0]);
			String movieID = user_movie_rating[1];
			String rating = user_movie_rating[2];
			context.write(new IntWritable(userID), new Text(movieID + ":" + rating));
			*/
			
			//value = userid \t movie1:rating, movie2:rating...
			//key = movie1:movie2 value = 1
			String line = value.toString().trim();
			String[] user_movieRatings = line.split("\t");
			
			String user = user_movieRatings[0];
			String [] movieRatings = user_movieRatings[1].split(",");
			//{movie1:rating, movie2:rating..}
			for (int i = 0; i < movieRatings.length; i++){
				String movie1 = movieRatings[i].trim().split(":")[0];
				
				for (int j = 0; j < movieRatings.length; j++){
					String movie2 = movieRatings[j].trim().split(":")[0];
					context.write(new Text(movie1 + ":" + movie2), new IntWritable(1));
				}
			}
		}
	}
	
	public static class CoOccurenceMatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException{
			/**
			//key = user_id value =<movieid:rating, movieid:rating ...>
			StringBuilder sb = new StringBuilder();
			while (values.iterator().hasNext()){
				sb.append(","+values.iterator().next().toString());
			}
			context.write(key, new Text(sb.toString().replaceFirst(",", "")));
			*/
			//key movie1:movie2 value = iterable<1,1,1>
			int sum = 0;
			while (values.iterator().hasNext()){
				sum += values.iterator().next().get();
			}
			context.write(key,  new IntWritable(sum));
		}
	}
	
	

}