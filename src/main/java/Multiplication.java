import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t movieA=relation
			//pass data to reducer
			String[] line = value.toString().trim().split("\t");
			// key is the midB, value is "midA=relation"
			context.write(new Text(line[0]), new Text(line[1].replace(':', '=')));

		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input: user,movie,rating
			//pass data to reducer

			String[] line = value.toString().trim().split(",");
			// key is the mid, value is "userid:rating"
			context.write(new Text(line[1]), new Text(line[0] + ":" + line[2]));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
			//collect the data for each movie, then do the multiplication

			//key is midA, value is the relation
			Map<String, Double> relation = new HashMap<String, Double>();
			// key is uid, value is the rating
			Map<String, Double> rating = new HashMap<String, Double>();

			for (Text t: values) {
				String line = t.toString();
				if (line.contains("=")) {
					// which means current value is the transposed relation list of midB
					String[] kv = line.trim().split("=");
					relation.put(kv[0], Double.parseDouble(kv[1]));
				} else {
					String[] kv = line.trim().split(":");
					rating.put(kv[0], Double.parseDouble(kv[1]));
				}
			}

			for (Map.Entry<String, Double> en1: relation.entrySet()) {
				for (Map.Entry<String, Double> en2: rating.entrySet()) {
					// key is uid:mid
					context.write(new Text(en1.getKey() + ":" + en2.getKey()), new DoubleWritable(en1.getValue()*en2.getValue()));
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);

		// redundancy?
		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath((job, new Path(args[2]));

		job.waitForCompletion(true);

	}
}
