package kmeansclustering;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

public class CalcJob extends WorkJob
{
	public enum Counters
	{
		PointChanged
	}

	public CalcJob(Configuration conf, String name, String inputPaths, String outputPath)
		throws IOException
	{
		super(conf, name);

		Job job = this.getJob();
		FileInputFormat.setInputPaths(job, new Path(inputPaths));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		MultipleOutputs.addNamedOutput(job, Output_Name_ClusterCenter, TextOutputFormat.class,
			LongWritable.class, PointWritable.class);
		MultipleOutputs.addNamedOutput(job, Output_Name_ClusterPoint, TextOutputFormat.class,
			LongWritable.class, PointWritable.class);

		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PointWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(PointWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(3);
	}

	public static class Map extends Mapper<LongWritable, Text, IntWritable, PointWritable>
	{
		private int             _clusterCount;
		private PointWritable[] _centers;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			Configuration config = context.getConfiguration();

			// Get cluster count...
			_clusterCount = config.getInt(Config_Name_Cluster_Count, 2);

			// Get cluster centers...
			_centers = new PointWritable[_clusterCount];
			URI[] cacheFiles = context.getCacheFiles();
			for(URI uri : cacheFiles)
			{
				BufferedReader fis = new BufferedReader(
					new FileReader(new File(uri.getPath()).getName()));

				String line;
				while((line = fis.readLine()) != null)
				{
					// 0	-29.0585 -43.2167
					// 1	-64.7473 21.8982
					// 2	-36.0366 -21.6135
					PointWritable point = new PointWritable();
					int cluster = parseOutput(config, line, point);

					_centers[cluster] = point;
				}
			}
		}

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException
		{
			String line = lineText.toString();
			if(line.length() == 0)
				return;

			int index = line.indexOf('\t');
			String clusterValue = line.substring(0, index);
			String pointValue = line.substring(index + 1);

			int cluster = Integer.parseInt(clusterValue);
			PointWritable point = PointWritable.parsePoint(pointValue);

			// Find the nearest center...
			int newCluster = 0;
			PointWritable center = _centers[0];
			double distance2 = point.distance2To(center);
			for(int i = 1; i < _centers.length; i++)
			{
				center = _centers[i];
				double d = point.distance2To(center);
				if(d < distance2)
				{
					distance2 = d;
					newCluster = i;
				}
			}

			if(newCluster != cluster)
			{
				context.getCounter(Counters.PointChanged).increment(1);
			}

			context.write(new IntWritable(newCluster), point);
		}
	}

	public static class Reduce
		extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable>
	{
		private MultipleOutputs<IntWritable, PointWritable> _outputs;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			_outputs = new MultipleOutputs<>(context);
		}

		@Override
		protected void reduce(IntWritable key, Iterable<PointWritable> values, Context context)
			throws IOException, InterruptedException
		{
			double x = 0;
			double y = 0;
			int count = 0;
			for(PointWritable value : values)
			{
				x += value.getX();
				y += value.getY();
				count++;
				_outputs.write(Output_Name_ClusterPoint, key, value);
			}

			_outputs.write(Output_Name_ClusterCenter, key, new PointWritable(x / count, y /
				count));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			_outputs.close();
			super.cleanup(context);
		}
	}
}
