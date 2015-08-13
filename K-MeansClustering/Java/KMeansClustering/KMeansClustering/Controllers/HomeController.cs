
using System.Threading.Tasks;
using System.Web.Mvc;
using KMeansClustering.Models;

namespace KMeansClustering.Controllers
{
	public class HomeController : Controller
	{
		public ActionResult Index()
		{
			return View();
		}

		public async Task<ActionResult> PointGroups()
		{
			ICalculator calculator = this.CreateCalculator();
			string[] groups = await calculator.GetPointGroupsAsync();

			return Json(groups, JsonRequestBehavior.AllowGet);
		}

		public async Task<ActionResult> Calc(string pointData = "", int clusterCount = 3)
		{
			ICalculator calculator = this.CreateCalculator();

			// Must call HDInsight APIs in another thread, otherwise it never returns.
			// https://hadoopsdk.codeplex.com/discussions/562447
			Stage[] stages = await Task.Run(async () => await calculator.CalculateAsync(pointData, clusterCount));

			return Json(stages, JsonRequestBehavior.AllowGet);
		}

		private ICalculator CreateCalculator()
		{
			return new HDInsightCalculator();
		}
	}
}
