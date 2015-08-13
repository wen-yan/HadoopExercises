
using System.Threading.Tasks;

namespace KMeansClustering.Models
{
	public class Point
	{
		public double X { get; set; }
		public double Y { get; set; }
		public int ClusterIndex { get; set; }
	}

	public class Stage
	{
		public Point[] Points { get; set; }
		public Point[] Centers { get; set; }
	}

	interface ICalculator
	{
		Task<string[]> GetPointGroupsAsync();
		Task<Stage[]> CalculateAsync(string pointGroup, int clusterCount);
	}
}
