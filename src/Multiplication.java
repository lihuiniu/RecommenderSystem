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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Multiplication {

	public Multiplication() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("coOcuurencePath", args[0]);
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MultiplicationMapper.class);
		job.setReducerClass(MultiplicationReducer.class);
		
		job.setJarByClass(Multiplication.class);
		
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

	public static class MultiplicationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		/**
		 * [movie1 {movie1, movie2, 8} {movie1, movie3, 5}{movie1,movie7,6}]
		 * [movie2 {movie2, movie1, 8} {movie2, movie3, 5}{movie2,movie7,6}
		 */
		
		Map<Integer, List<MovieRelation>> movieRelationMap = new HashMap<>();
		Map<Integer, Integer> denominator = new HashMap<>();

		@Override
		protected void setup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			String filePath = conf.get("coOccurencePath");
			Path pt = new Path(filePath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			
			while (line != null){
				String[] tokens = line.toString().trim().split("\t");
				String[] movies = tokens[0].split(":");
				
				int movie1 = Integer.parseInt(movies[0]);
				int movie2 = Integer.parseInt(movies[1]);
				int relation = Integer.parseInt(tokens[1]);
				
				MovieRelation movieRalation =new MovieRelation(movie1,movie2,relation);
				if (movieRelationMap.containsKey(movie1)){
					MovieRelation movieRelation = new MovieRelation(movie1, movie2, relation);
					movieRelationMap.get(movie1).add(movieRelation);
				} else {
					List<MovieRelation> newList = new ArrayList<>();
					MovieRelation movieRelation = new MovieRelation(movie1, movie2, relation);
					newList.add(movieRelation);
					movieRelationMap.put(movie1, newList);
				}
				line = br.readLine();
			}

			br.close();
			
			for (Map.Entry<Integer, String> mpe :)
			}
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
			double rating = Double.parseDouble(tokens[2]);
			
			for (MovieRelation relation : movieRelationMap.get(movie)){
				double score = rating * relation.getRelation();
				//map.get(movie2).sum(relation) = fenmu
				//map(movie_id,sum)
				
				//normalize
				score = score / denominator.get(relation.getMovie2());
				DecimalFormat df = new DecimalFormat("#.00");
				score = Double.valueOf(df.format(score));
				
				//context.write(new IntWritable(user), new Text(relation.getMovie2() + ":" + score));
				context.write(new Text(user+ ":"+relation.getMovie2()), new DoubleWritable(score));
				
				//user movietag:score
				// -->user + movietag  score
			}
			
		}
	}
	
	public static class MultiplicationReducer extends Reducer< Text, DoubleWritable, IntWritable, Text>{
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
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