package kmeansclustering;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.logging.Logger;

public class KMeansClusteringJob extends Configured implements Tool
{
	public static final Logger LOG = Logger.getGlobal();

	public static void main(String[] args) throws Exception
	{
		LOG.info("main() started");

		int res = ToolRunner.run(new KMeansClusteringJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{
		String inputPaths = args[0];
		String outputPathBase = args[1];

		// Create job for initialize...
		String outputPath = outputPathBase + "/0";

		LOG.info("Starting initialize job...");
		LOG.info("Input paths : " + inputPaths);
		LOG.info("Output path : " + outputPath);

		InitializeJob job = new InitializeJob(getConf(), "initialize", inputPaths, outputPath);

		if(!job.getJob().waitForCompletion(true))
			return 2;

		for(int i = 0; i < 4; i++)
		{
			String pointFiles = outputPathBase + "/" + Integer
				.toString(i) + "/" + WorkJob.Output_Name_ClusterPoint + "-*";
			outputPath = outputPathBase + "/" + Integer.toString(i + 1);
			CalcJob calcJob = new CalcJob(getConf(), "calculate", pointFiles, outputPath);

			LOG.info("Starting calculate job " + Integer.toString(i) + "...");
			LOG.info("Input paths : " + pointFiles);
			LOG.info("Output path : " + outputPath);

			// Add cluster center file as cached file...
			String clusterCenterFiles = outputPathBase + "/" + Integer
				.toString(i) + "/" + WorkJob.Output_Name_ClusterCenter + "-*";

			// Cannot use 'FileSystem fs = FileSystem.get(getConf());' directly,
			// because it uses default file system schema, which is HDFS. In fact,
			// it just calls FileSystem.get(getDefaultUri(conf), conf). The default
			// getDefaultUri(conf) is defined by fs.default.name or fs.defaultFS.
			// To adapt to other file system, such as Azure Storage, using URI,
			// which contains schema information to create FileSystem instance.
			Path clusterCenterFilesPath = new Path(clusterCenterFiles);
			FileSystem fs = FileSystem.get(clusterCenterFilesPath.toUri(), getConf());
			FileStatus[] status = fs.globStatus(clusterCenterFilesPath);

			for(int j = 0; j < status.length; j++)
			{
				LOG.info("Add cache file : " + status[j].getPath().toUri());
				calcJob.getJob().addCacheFile(status[j].getPath().toUri());
			}

			if(!calcJob.getJob().waitForCompletion(true))
				return 100000 + i;
		}

		return 0;
	}
}
