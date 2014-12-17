package org.myorg;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public final class HighestSale {
	
	public static final class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final Text word = new Text();

		public final void map(final LongWritable key, final Text value, final Context context)
				throws IOException, InterruptedException {
			final String line = value.toString();
			final String[] parts = line.trim().split("\t");
			if (parts.length == 6) {
				final String product = parts[2];
				final double sales = Double.parseDouble(parts[4]);
				word.set(product);
				context.write(word, new DoubleWritable(sales));
			}
		}
	}

	public static final class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public final void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context)
				throws IOException, InterruptedException {
			double maxSale = 0.0;
			double currentSale;
			for (DoubleWritable val : values) {
				currentSale = val.get();
				if (maxSale < currentSale) {
					maxSale = currentSale;
				}
			}
			context.write(key, new DoubleWritable(maxSale));
		}
	}

	public final static void main(final String[] args) throws Exception {
		final Configuration conf = new Configuration();
		final Job job = new Job(conf, "HighestSale");
		job.setJarByClass(HighestSale.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}


}