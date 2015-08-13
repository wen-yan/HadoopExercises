
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.Hadoop.Client;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace KMeansClustering.Models
{
	public class HDInsightCalculator : ICalculator
	{
		private const string ContainerName = "hdpcls";

		#region ICalculator Members

		public async Task<string[]> GetPointGroupsAsync()
		{
			CloudStorageAccount storageAccount =
					CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));

			// Create a blob client for interacting with the blob service.
			CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

			// Get container...
			CloudBlobContainer container = blobClient.GetContainerReference(ContainerName);

			// List all the blobs in the container 
			List<string> names = new List<string>();
			foreach(IListBlobItem blob in container.ListBlobs("KMeans/Input/"))
			{
				// Blob type will be CloudBlockBlob, CloudPageBlob or CloudBlobDirectory
				// Use blob.GetType() and cast to appropriate type to gain access to properties specific to each type
				int index = blob.Uri.PathAndQuery.LastIndexOf("/");
				if(index < 0)
					continue;

				names.Add(blob.Uri.PathAndQuery.Substring(index + 1));
			}
			return names.ToArray();
		}

		public async Task<Stage[]> CalculateAsync(string pointGroup, int clusterCount)
		{
			BasicAuthCredential creds = new BasicAuthCredential();
			creds.UserName = "hadoop";
			creds.Password = "";
			creds.Server = new Uri("http://localhost:50111");

			// Create a hadoop client to connect to HDInsight
			IJobSubmissionClient jobClient = JobSubmissionClientFactory.Connect(creds);

			// Delete outputs...
			this.DeleteOutputs();

			// Start job...
			JobCreationResults mrJobResults = this.StartJob(jobClient, pointGroup, clusterCount);

			// Wait for the job to complete
			WaitForJobCompletion(mrJobResults, jobClient);

			// Get result...
			Stage[] stages = this.GetResult();

			return stages;
		}

		#endregion

		private void DeleteOutputs()
		{
			CloudStorageAccount storageAccount =
				CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
			CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
			CloudBlobContainer blobContainer = blobClient.GetContainerReference(ContainerName);


			List<string> outputBlobNames = blobContainer.ListBlobs("KMeans/Output", true)
				.OfType<ICloudBlob>().Select(x => x.Name).ToList();
			//outputBlobNames.Add("KMeans/Output");

			foreach(string outputBlobName in outputBlobNames)
			{
				CloudBlockBlob outputBlob = blobContainer.GetBlockBlobReference(outputBlobName);
				if(outputBlob != null)
					outputBlob.DeleteIfExists();
			}
		}

		private JobCreationResults StartJob(IJobSubmissionClient jobClient, string pointGroup, int clusterCount)
		{
			// Define the MapReduce job
			MapReduceJobCreateParameters mrJobDefinition = new MapReduceJobCreateParameters()
			{
				JarFile = "wasb://hdpcls@storageemulator/KMeans/App/KMeansClustering.jar",
				ClassName = "kmeansclustering.KMeansClusteringJob",
			};
			mrJobDefinition.Defines.Add("kmeans.cluster.count", clusterCount.ToString());

			mrJobDefinition.Arguments.Add("wasb://hdpcls@storageemulator/KMeans/Input/" + pointGroup);
			mrJobDefinition.Arguments.Add("wasb://hdpcls@storageemulator/KMeans/Output");

			// Run the MapReduce job
			JobCreationResults mrJobResults = jobClient.CreateMapReduceJob(mrJobDefinition);
			return mrJobResults;
		}

		private static void WaitForJobCompletion(JobCreationResults jobResults, IJobSubmissionClient client)
		{
			JobDetails jobInProgress = client.GetJob(jobResults.JobId);
			while(jobInProgress.StatusCode != JobStatusCode.Completed && jobInProgress.StatusCode != JobStatusCode.Failed)
			{
				jobInProgress = client.GetJob(jobInProgress.JobId);
				Thread.Sleep(TimeSpan.FromSeconds(10));
			}
		}

		private Stage[] GetResult()
		{
			CloudStorageAccount storageAccount =
				CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
			CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
			CloudBlobContainer blobContainer = blobClient.GetContainerReference(ContainerName);


			List<Stage> stages = new List<Stage>();
			List<string> stageBlobNames = blobContainer.ListBlobs("KMeans/Output/")
				.OfType<ICloudBlob>().Select(x => x.Name).ToList();

			foreach(string stageBlobName in stageBlobNames)
			{
				Stage stage = new Stage();

				stage.Points = this.GetPoints(blobContainer, stageBlobName + "/ClusterPoint");
				stage.Centers = this.GetPoints(blobContainer, stageBlobName + "/ClusterCenter");
				stages.Add(stage);
			}
			return stages.ToArray();
		}

		private Point[] GetPoints(CloudBlobContainer blobContainer, string prefix)
		{
			List<Point> points = new List<Point>();
			foreach(ICloudBlob blob in blobContainer.ListBlobs(prefix).OfType<ICloudBlob>())
			{
				string blobName = blob.Name;

				using(MemoryStream stream = new MemoryStream())
				{
					CloudBlockBlob blockBlob = blobContainer.GetBlockBlobReference(blobName);
					blockBlob.DownloadToStream(stream);
					stream.Position = 0;

					using(TextReader reader = new StreamReader(stream))
					{
						while(true)
						{
							string line = reader.ReadLine();
							if(line == null)
								break;

							string[] fields = line.Split(' ', '\t');
							Point point = new Point()
							{
								ClusterIndex = int.Parse(fields[0]),
								X = double.Parse(fields[1]),
								Y = double.Parse(fields[2]),
							};
							points.Add(point);
						}
					}
				}
			}
			return points.ToArray();
		}
	}
}