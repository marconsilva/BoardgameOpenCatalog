using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using HtmlAgilityPack;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic.CompilerServices;

namespace DICE.Data.Ingest.Bgg.BoardgameDataImport
{
    public static class BoardgameGeekGameListScrapperScheduled
    {
        private static bool clearQueueBeforeStart;

        [FunctionName("BoardgameGeekGameListScrapperScheduled")]
        public static async void Run([TimerTrigger("0 0 0 * * *", RunOnStartup = true)] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.UtcNow}");

            LoadEnvironmentSettings();
            await BoardgameGeekGameListScrapper.ImportData(log, true, false, clearQueueBeforeStart, false, null, null);
        }




        private static void LoadEnvironmentSettings()
        {
            clearQueueBeforeStart = bool.Parse(System.Environment.GetEnvironmentVariable("ClearNewItemsQueueAtStart", EnvironmentVariableTarget.Process));
        }

    }
}
