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
    public static class BoardgameGeekGameListScrapper
    {
        private static string boardgameListURLFormat;
        private static string storageAccountConnectionString;
        private static string queueName;
        private static string tableName;
        private static string boardgamePartitionKey;
        private static int maxRetryAttempts;
        private static int maxPageLimit;
        private static int startPageNumber;
        private static int retryAttemptIntervalInMiliseconds;
        private static bool queueNewItemsForUpdate;
        private static bool queueAllItemsForUptate;
        private static bool clearQueueBeforeStart;
        private static bool clearTableBeforeStart;
        private static QueueClient queueClient;
        private static CloudStorageAccount storageAccount;
        private static CloudTableClient tableClient;
        private static CloudTable cloudTable;

        public static async Task ImportData(ILogger log, bool queueNewItemsForUpdate, bool queueAllItemsForUptate, 
            bool clearQueueBeforeStart, bool clearTableBeforeStart, int? maxPageLimitParam, int? startPageNumberParam)
        {
            BoardgameGeekGameListScrapper.queueNewItemsForUpdate = queueNewItemsForUpdate;
            BoardgameGeekGameListScrapper.queueAllItemsForUptate = queueAllItemsForUptate;
            BoardgameGeekGameListScrapper.clearQueueBeforeStart = clearQueueBeforeStart;
            BoardgameGeekGameListScrapper.clearTableBeforeStart = clearTableBeforeStart;
            LoadEnvironmentSettings();
            await LoadDependencies(log, clearQueueBeforeStart);

            if (maxPageLimitParam != null && maxPageLimitParam.HasValue)
                maxPageLimit = maxPageLimitParam.Value;

            if (maxPageLimitParam != null && startPageNumberParam.HasValue)
                startPageNumber = startPageNumberParam.Value;

            List<BoardgameReferenceTableListEntity> fullList = new List<BoardgameReferenceTableListEntity>();
            HtmlWeb web = new HtmlWeb();
            var currentPage = web.Load(string.Format(boardgameListURLFormat, 1));

            string lastPageCountText = currentPage.DocumentNode.SelectSingleNode($"//*[@id=\"maincontent\"]/form/div/div[1]/a[last()]").InnerText;
            int lastPageCount = int.Parse(lastPageCountText[1..^1]);

            log.LogInformation($"Found {lastPageCount} Pages to process");

            if (maxPageLimit > 0 && maxPageLimit < lastPageCount)
            {
                log.LogInformation($"MaxPage Limit is Set to {maxPageLimit}");
                lastPageCount = maxPageLimit;
            }

            if (startPageNumber > 0)
            {
                log.LogInformation($"Start Page is Set to {startPageNumber}");

            }

            log.LogInformation($"Processing Page 1");

            await ExtractDataFromPage(fullList, currentPage, 1, log);

            var result = Parallel.For(2, lastPageCount + 1, new ParallelOptions() { MaxDegreeOfParallelism = 4 }, async (i, state) =>
            {
                int retryAttemptCounter = -1;
                while (retryAttemptCounter < maxRetryAttempts)
                {
                    try
                    {
                        HtmlWeb webRequest = new HtmlWeb();
                        var currentPageResponse = web.Load(string.Format(boardgameListURLFormat, i));

                        log.LogInformation($"Processing Page {i}");

                        await ExtractDataFromPage(fullList, currentPageResponse, i, log);
                        retryAttemptCounter = int.MaxValue;
                    }
                    catch (Exception e)
                    {
                        log.LogWarning($"Failed to process Page {i} due to error;");
                        log.LogWarning($"Exception Details: {e.Message}");
                        retryAttemptCounter++;
                        log.LogInformation($"Page {i} Retry will Start in {retryAttemptIntervalInMiliseconds} Miliseconds");
                        await Task.Delay(retryAttemptIntervalInMiliseconds);
                        if (retryAttemptCounter >= maxRetryAttempts)
                            log.LogError($"Page {i} Has Reached Max Retry Attempts");
                        else
                            log.LogInformation($"Page {i} Retry will Start Now");

                    }
                }

            });


        }

        private static async Task ExtractDataFromPage(List<BoardgameReferenceTableListEntity> fullList, HtmlDocument currentPage, int pageNumber, ILogger log)
        {
            var tableRows = currentPage.DocumentNode.SelectNodes("//*[@id=\"collectionitems\"]/tr");

            foreach (var item in tableRows)
            {
                BoardgameReferenceTableListEntity parsedElement = ParseRow(item, log);

                if (parsedElement != null)
                {
                    log.LogDebug($"Found {parsedElement.Title} - (BGG ID - {parsedElement.Id}) - Page {pageNumber}");
                    bool isNewEntry = await CreateOrUpdateEntryTableIfNew(parsedElement, pageNumber, log);
                    if (isNewEntry && queueNewItemsForUpdate || queueAllItemsForUptate)
                        WriteElementToStorageQueue(parsedElement, pageNumber, log);
                    fullList.Add(parsedElement);
                }
            }
        }

        private static async Task<bool> CreateOrUpdateEntryTableIfNew(BoardgameReferenceTableListEntity parsedElement, int pageNumber, ILogger log)
        {
            if (parsedElement == null)
                return false;

            try
            {
                TableOperation retrieveOperation = TableOperation.Retrieve<BoardgameReferenceTableListEntity>(boardgamePartitionKey, parsedElement.RowKey);
                TableResult retrieveOperationResult = await cloudTable.ExecuteAsync(retrieveOperation);
                BoardgameReferenceTableListEntity entity = retrieveOperationResult.Result as BoardgameReferenceTableListEntity;

                if (entity != null)
                    return false;

                TableOperation insertOrMergeOperation = TableOperation.InsertOrMerge(parsedElement);
                TableResult insertOrMergeOperationResult = await cloudTable.ExecuteAsync(insertOrMergeOperation);

                return true;
            }
            catch (StorageException e)
            {
                log.LogError($"Error Trying to add or update element {parsedElement.Id}: {e.Message}");
                return false;
            }
        }

        private static void WriteElementToStorageQueue(BoardgameReferenceTableListEntity parsedElement, int pageNumber, ILogger log)
        {
            if (queueClient.Exists())
            {
                queueClient.SendMessage(Newtonsoft.Json.JsonConvert.SerializeObject(parsedElement));
                log.LogDebug($"Added Element to que {parsedElement.Id} from page {pageNumber}");
            }
            else
            {
                log.LogInformation($"Queue Not Initialized: Did not add element {parsedElement.Id} from page {pageNumber}");
            }
        }

        private static BoardgameReferenceTableListEntity ParseRow(HtmlNode tableRow, ILogger log)
        {
            BoardgameReferenceTableListEntity result = new BoardgameReferenceTableListEntity();
            var rowElements = tableRow.Descendants("td");

            if (rowElements == null || rowElements.Count() == 0)
                return null;
            if (int.TryParse(rowElements.ElementAt(0).InnerText.Trim(), out int BGGRankingNumber))
                result.BGGRankingNumber = BGGRankingNumber;
            else
                result.BGGRankingNumber = -1;

            result.BGGPageUrl = rowElements.ElementAt(2).Descendants("a").FirstOrDefault()?.GetAttributeValue("href", "");
            result.Title = rowElements.ElementAt(2).Descendants("a").FirstOrDefault()?.InnerText;

            var yearString = rowElements.ElementAt(2).Descendants("span").FirstOrDefault()?.InnerText;
            if (!string.IsNullOrWhiteSpace(yearString) && int.TryParse(yearString[1..^1], out int Year))
                result.Year = Year;
            else
                result.Year = -1;

            if (float.TryParse(rowElements.ElementAt(3).InnerText.Trim(), System.Globalization.NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out float GeekRating))
                result.GeekRating = GeekRating;
            else
                result.GeekRating = 0;

            if (float.TryParse(rowElements.ElementAt(4).InnerText.Trim(), System.Globalization.NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out float AverageRating))
                result.AverageRating = AverageRating;
            else
                result.AverageRating = 0;

            if (long.TryParse(rowElements.ElementAt(5).InnerText.Trim(), System.Globalization.NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out long NumberOfVoters))
                result.NumberOfVoters = NumberOfVoters;
            else
                result.NumberOfVoters = 0;

            result.Id = int.Parse(Regex.Match(result.BGGPageUrl, "[\\/]([0-9]+)[\\/]").Groups[1].Value);

            result.RowKey = result.Id.ToString();
            result.PartitionKey = boardgamePartitionKey;

            return result;
        }

        private static void LoadEnvironmentSettings()
        {
            boardgameListURLFormat = System.Environment.GetEnvironmentVariable("BGGAllBoardgameListURLFormat", EnvironmentVariableTarget.Process);
            storageAccountConnectionString = System.Environment.GetEnvironmentVariable("StorageAccountConnectionString", EnvironmentVariableTarget.Process);
            queueName = System.Environment.GetEnvironmentVariable("StorageAccountGameDataUpdateQueueName", EnvironmentVariableTarget.Process);
            tableName = System.Environment.GetEnvironmentVariable("StorageAccountGameReferenceTableName", EnvironmentVariableTarget.Process);
            boardgamePartitionKey = System.Environment.GetEnvironmentVariable("StorageAccountBoardgameItemPartitionKey", EnvironmentVariableTarget.Process);
            maxRetryAttempts = int.Parse(System.Environment.GetEnvironmentVariable("MaxRetryAttempts", EnvironmentVariableTarget.Process));
            retryAttemptIntervalInMiliseconds = int.Parse(System.Environment.GetEnvironmentVariable("RetryAttemptDelayInMiliseconds", EnvironmentVariableTarget.Process));
            maxPageLimit = int.Parse(System.Environment.GetEnvironmentVariable("MaxPageLimit", EnvironmentVariableTarget.Process));
            startPageNumber = int.Parse(System.Environment.GetEnvironmentVariable("StartPageNumber", EnvironmentVariableTarget.Process));

        }

        private static async Task LoadDependencies(ILogger log, bool clearQueueBeforeStart)
        {
            queueClient = new QueueClient(storageAccountConnectionString, queueName);

            // Create the queue if it doesn't already exist
            queueClient.CreateIfNotExists();

            if (queueClient.Exists() && clearQueueBeforeStart)
                queueClient.ClearMessages();
            try
            {
                storageAccount = CloudStorageAccount.Parse(storageAccountConnectionString);
            }
            catch (FormatException)
            {
                log.LogError("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the application.");
            }
            catch (ArgumentException)
            {
                log.LogError("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the sample.");
            }

            tableClient = storageAccount.CreateCloudTableClient(new TableClientConfiguration());
            cloudTable = tableClient.GetTableReference(tableName);

            if (await cloudTable.CreateIfNotExistsAsync())
            {
                log.LogInformation("Created Table named: {0}", tableName);
            }
            else
            {
                log.LogInformation("Table {0} found and connected", tableName);
            }
        }
    }
}
