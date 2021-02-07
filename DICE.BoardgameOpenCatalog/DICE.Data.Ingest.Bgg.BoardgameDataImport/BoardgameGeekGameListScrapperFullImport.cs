using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace DICE.Data.Ingest.Bgg.BoardgameDataImport
{
    public static class BoardgameGeekGameListScrapperFullImport
    {
        private static bool clearQueueBeforeStart;

        [FunctionName("BoardgameGeekGameListScrapperFullImport")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation($"C# HTTP trigger function processed a request at {DateTime.UtcNow}.");

            LoadEnvironmentSettings();
            string maxPageLimitParam = req.Query["maxPageLimit"];
            string startPageNumberParam = req.Query["startPageNumber"];

            int? maxPageLimit = null, startPageNumber = null;
            if (!string.IsNullOrEmpty(maxPageLimitParam) && int.TryParse(maxPageLimitParam, out int maxPageLimitParsed))
                maxPageLimit = maxPageLimitParsed;

            if (!string.IsNullOrEmpty(startPageNumberParam) && int.TryParse(startPageNumberParam, out int startPageNumberParsed))
                startPageNumber = startPageNumberParsed;


            await BoardgameGeekGameListScrapper.ImportData(log, true, true, clearQueueBeforeStart, false, maxPageLimit, startPageNumber);
            
            return new OkObjectResult("{Sucess: true}");
        }

        private static void LoadEnvironmentSettings()
        {
            clearQueueBeforeStart = bool.Parse(System.Environment.GetEnvironmentVariable("ClearNewItemsQueueAtStart", EnvironmentVariableTarget.Process));
        }
    }
}
