
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace KMeansClustering.Models
{
	//class HadoopCommandLineCalculator : ICalculator
	//{
	//	private static Regex PointGroupFileNameRegex =
	//		new Regex(@"^ .* /KMeans/Input/(?<Name>.*\.txt)",
	//			RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);

	//	public string[] PointGroups
	//	{
	//		get
	//		{
	//			using(Process process = new Process())
	//			{
	//				process.StartInfo = new ProcessStartInfo(
	//					@"C:\hdp\hadoop-2.4.0.2.1.3.0-1981\bin\hadoop.cmd",
	//					"fs -ls \"/KMeans/Input");
	//				process.StartInfo.RedirectStandardOutput = true;
	//				process.StartInfo.RedirectStandardError = true;
	//				process.StartInfo.UseShellExecute = false;
	//				process.Start();

	//				StringBuilder standardOutput = new StringBuilder();

	//				// read chunk-wise while process is running.
	//				while(!process.HasExited)
	//				{
	//					standardOutput.Append(process.StandardOutput.ReadToEnd());
	//				}
	//				// make sure not to miss out on any remaindings.
	//				standardOutput.Append(process.StandardOutput.ReadToEnd());


	//				using(TextReader reader = new StringReader(standardOutput.ToString()))
	//				{
	//					List<string> groups = new List<string>();
	//					while(true)
	//					{
	//						string line = reader.ReadLine();
	//						if(line == null)
	//							break;

	//						Match match = PointGroupFileNameRegex.Match(line);
	//						if(!match.Success)
	//							continue;

	//						string name = match.Groups["Name"].Value;
	//						groups.Add(name);
	//					}
	//					return groups.ToArray();
	//				}
	//			}
	//		}
	//	}

	//	public Point[] Calculate(string pointGroup, int clusterCount)
	//	{
	//		return null;
	//	}
	//}
}
