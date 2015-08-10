package kmeansclustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class InitializeJob extends WorkJob
{
	public InitializeJob(Configuration conf, String name, String inputPaths, String outputPath)
		throws IOException
	{
		super(conf, name);

		Job job = this.getJob();
		FileInputFormat.setInputPaths(job, new Path(inputPaths));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		MultipleOutputs.addNamedOutput(job, Output_Name_ClusterCenter, TextOutputFormat.class,
			IntWritable.class, PointWritable.class);
		MultipleOutputs.addNamedOutput(job, Output_Name_ClusterPoint, TextOutputFormat.class,
			IntWritable.class, PointWritable.class);

		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(PointWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(PointWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(3);
	}

	public static class Map extends Mapper<LongWritable, Text, NullWritable, PointWritable>
	{
		@Override
		public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException
		{
			String line = lineText.toString();
			if(line.startsWith("#"))    // # is for comment...
				return;

			line = line.trim();
			if(line.length() == 0)
				return;

			PointWritable point = PointWritable.parsePoint(line);
			context.write(NullWritable.get(), point);
		}
	}

	public static class Reduce
		extends Reducer<NullWritable, PointWritable, IntWritable, PointWritable>
	{
		private int                                          _clusterCount;
		private MultipleOutputs<IntWritable, PointWritable> _outputs;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			Configuration config = context.getConfiguration();
			_clusterCount = config.getInt(Config_Name_Cluster_Count, 2);
			_outputs = new MultipleOutputs<>(context);
		}

		@Override
		protected void reduce(NullWritable key, Iterable<PointWritable> values, Context context)
			throws IOException, InterruptedException
		{
			ArrayList<PointWritable> points = new ArrayList<>();
			for(PointWritable value : values)
			{
				points.add(value.clonePoint());
				_outputs.write(Output_Name_ClusterPoint, new IntWritable(-1), value);
			}

			Random rand = new Random();
			for(int i = 0; i < _clusterCount; i++)
			{
				int index = rand.nextInt(points.size());
				PointWritable point = points.get(index);
				points.remove(index);
				_outputs.write(Output_Name_ClusterCenter, new IntWritable(i), point);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			_outputs.close();
			super.cleanup(context);
		}
	}
}
