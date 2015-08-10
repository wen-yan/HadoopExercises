package kmeansclustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public abstract class WorkJob
{
	public static final String Config_Name_Cluster_Count = "kmeans.cluster.count";

	public static final String Output_Name_ClusterCenter = "ClusterCenter";
	public static final String Output_Name_ClusterPoint  = "ClusterPoint";

	private final Job _job;

	public WorkJob(Configuration conf, String name) throws IOException
	{
		_job = Job.getInstance(conf, name);
		_job.setJarByClass(this.getClass());
	}

	public Job getJob()
	{
		return _job;
	}

	protected static int parseOutput(Configuration config, String line, PointWritable point)
	{
		String keyValueSeparator = config.get(TextOutputFormat.SEPERATOR, "\t");
		int index = line.indexOf(keyValueSeparator);
		String clusterValue = line.substring(0, index);
		String pointValue = line.substring(index + keyValueSeparator.length());

		point.parse(pointValue);
		return Integer.parseInt(clusterValue);
	}
}
