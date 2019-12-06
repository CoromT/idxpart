using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace IdxPart
{
    public class Program
    {
        // Clients
        private static ISearchServiceClient _searchClient;
        private static HttpClient _httpClient = new HttpClient();
        private static string _searchServiceEndpoint;
        private static string indexerTemplate;
        private static string datasourceTemplate;
        private static string _apiKey;
        private static int _batchCounter;
        private static long _durationMS;

        public static void Main(string[] args)
        {
            try
            {
                Program.Main1(args);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        private static string GetSanitizedValue(string keyName) =>
            keyName.Contains("ApiKey") || keyName.Contains("ConnectionString") ? "..." : ConfigurationManager.AppSettings[keyName];

        public static void Main1(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = 100;

            if (args.Length == 0)
            {
                var parameters = string.Join("\n              ", ConfigurationManager.AppSettings.AllKeys.Select(k => "/" + k + ":" + GetSanitizedValue(k)));
                Console.WriteLine(@"Manages partitioned Azure Search indexers for bulk importing large amounts of data

USAGE:  idxpart  [instances] action [action] [/Parameter:Value]

  instances:  indexer instances to apply action to (default all).  May be specified muliple times as a single number or range:  e.g. 1 3-6 12 
  
  actions:    create    (in the disabled state)
              delete
              enable
              disable
              run
              status
              import

  parameters: Values may be set in config or overwriten via commandline
              " + parameters + @"

EXAMPLES:
    idxpart create
    idxpart delete
    idxpart create enable
    idxpart status
    idxpart disable 2 5-6
    idxpart enable /SkipValidation:true     
    idxpart create enable /DatasourceTemplate:datasource-incremental-cosmos.json /PartitionCount:1
    idxpart import

", parameters);
                return;
            }

            OverrideCongifWithArguments(args);

            string searchServiceName = ConfigurationManager.AppSettings["SearchServiceName"];
            _apiKey = ConfigurationManager.AppSettings["SearchServiceApiKey"];

            indexerTemplate = File.ReadAllText(ConfigurationManager.AppSettings["IndexerTemplate"]);
            datasourceTemplate = File.ReadAllText(ConfigurationManager.AppSettings["DatasourceTemplate"]);

            _searchClient = new SearchServiceClient(searchServiceName, new SearchCredentials(_apiKey));

            _httpClient.DefaultRequestHeaders.Add("api-key", _apiKey);

            _searchServiceEndpoint = String.Format("https://{0}.{1}", searchServiceName, _searchClient.SearchDnsSuffix);

            bool result = RunAsync(args).GetAwaiter().GetResult();

            Console.WriteLine();
            if (!result)
            {
                Console.WriteLine("Something went wrong.");
            }
            else
            {
                Console.WriteLine("All operations were successful.");
            }

            // pause for convienence while debugging so you can read the console output
            if (System.Diagnostics.Debugger.IsAttached)
            {
                Console.WriteLine("Press any key to exit.");
                Console.ReadKey();
            }
        }

        private static void OverrideCongifWithArguments(string[] args)
        {
            Regex cmdRegEx = new Regex(@"/(?<name>.+?):(?<val>.+)");

            Dictionary<string, string> cmdArgs = new Dictionary<string, string>();
            foreach (string s in args)
            {
                Match m = cmdRegEx.Match(s);
                if (m.Success)
                {
                    ConfigurationManager.AppSettings[m.Groups[1].Value] = m.Groups[2].Value;
                }
            }
        }

        private static int[] GetNumberRangesFromArguments(string[] args)
        {
            List<int> numbers = new List<int>();
            Regex cmdRegEx = new Regex(@"^(?<start>\d+)(?:-(?<val>\d+))?$");

            Dictionary<string, string> cmdArgs = new Dictionary<string, string>();
            foreach (string s in args)
            {
                Match m = cmdRegEx.Match(s);
                if (m.Success)
                {
                    int val = int.Parse(m.Groups[1].Value);
                    if (m.Groups[2].Success)
                    {
                        int val2 = int.Parse(m.Groups[2].Value);
                        numbers.AddRange(Enumerable.Range(Math.Min(val, val2), Math.Abs(val2 - val) + 1));
                    }
                    else
                    {
                        numbers.Add(val);
                    }
                }
            }

            return numbers.ToArray();
        }

        private static async Task<bool> RunAsync(string[] args)
        {
            bool result = true;

            int instances = int.Parse(ConfigurationManager.AppSettings["PartitionCount"] ?? "1");

            var instanceFilter = GetNumberRangesFromArguments(args);

            Console.WriteLine("Indexer Partitions: {0}", instances);

            // use numeric partitioning by default
            Func<object, object> getNextPartitionValue = (object value) => (int)value + 1;
            Func<object, string> formatPartitionValue = (object value) => ((int)value).ToString(ConfigurationManager.AppSettings["PartitionFormatString"] ?? "0");
            object partitionStartValue = 1;
            object partitionEndValue;

            // use a date time partitioner if Partition Dates are used
            if (!string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["PartitionStartDate"]) && !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["PartitionEndDate"]))
            {
                DateTimeOffset partitionStartDate, partitionEndDate;
                if (!DateTimeOffset.TryParse(ConfigurationManager.AppSettings["PartitionStartDate"], out partitionStartDate))
                    partitionStartDate = DateTimeOffset.FromUnixTimeSeconds(long.Parse(ConfigurationManager.AppSettings["PartitionStartDate"]));
                if (!DateTimeOffset.TryParse(ConfigurationManager.AppSettings["PartitionEndDate"], out partitionEndDate))
                    partitionEndDate = DateTimeOffset.FromUnixTimeSeconds(long.Parse(ConfigurationManager.AppSettings["PartitionEndDate"]));

                TimeSpan interval = TimeSpan.FromTicks((long)((partitionEndDate - partitionStartDate).Ticks / (double)instances));
                getNextPartitionValue = (object value) => (DateTimeOffset)value + interval;
                formatPartitionValue = (object value) => ((DateTimeOffset)value).ToUnixTimeSeconds().ToString();
            }

            for (int i = 1; i <= instances; i++)
            {
                if (instanceFilter.Any() && !instanceFilter.Contains(i))
                {
                    continue;
                }

                partitionEndValue = getNextPartitionValue(partitionStartValue);
                string suffix = instances > 1 ? "-" + instances.ToString("000") + "-" + i.ToString("000") : string.Empty;
                string iName = ConfigurationManager.AppSettings["IndexerNamePrefix"] + suffix;
                string dsName = string.IsNullOrEmpty(ConfigurationManager.AppSettings["DatasourceNamePrefix"]) ? iName : ConfigurationManager.AppSettings["DatasourceNamePrefix"] + suffix;
                string description = string.Format("Partition {0} of {1} from {2} to {3} ({4}-{5})", i, instances, partitionStartValue, partitionEndValue, formatPartitionValue(partitionStartValue), formatPartitionValue(partitionEndValue));

                if (args.Contains("delete", StringComparer.InvariantCultureIgnoreCase) || args.Contains("del", StringComparer.InvariantCultureIgnoreCase))
                {
                    result = await DeleteObject(iName, "indexers") && await DeleteObject(dsName, "datasources");
                    if (!result)
                        return result;
                }

                if (args.Contains("create", StringComparer.InvariantCultureIgnoreCase))
                {
                    Console.WriteLine();
                    Console.WriteLine(description);
                    result = await CreateDataSource(dsName, description, formatPartitionValue(partitionStartValue), formatPartitionValue(partitionEndValue));
                    if (!result)
                        return result;

                    result = await CreateIndexer(iName, dsName, description, disabled: true);
                    if (!result)
                        return result;
                }

                if (args.Contains("disable", StringComparer.InvariantCultureIgnoreCase))
                {
                    result = await CreateIndexer(iName, dsName, description, disabled: true);
                    if (!result)
                        Console.WriteLine("Could not disable " + iName);
                }

                if (args.Contains("enable", StringComparer.InvariantCultureIgnoreCase))
                {
                    result = await CreateIndexer(iName, dsName, description, disabled: false);
                    if (!result)
                        Console.WriteLine("Could not enable " + iName);
                }

                if (args.Contains("reset", StringComparer.InvariantCultureIgnoreCase))
                {
                    result = await ResetIndexer(iName);
                    Console.WriteLine("Could not reset " + iName);
                }

                if (args.Contains("run", StringComparer.InvariantCultureIgnoreCase))
                {
                    result = await RunIndexer(iName);
                    if (!result)
                        Console.WriteLine("Could not run " + iName);
                }

                if (args.Contains("status", StringComparer.InvariantCultureIgnoreCase) || args.Contains("stats", StringComparer.InvariantCultureIgnoreCase) || args.Contains("stat", StringComparer.InvariantCultureIgnoreCase))
                {

                    var stat = await GetIndexerStatus(iName);
                    IndexerExecutionResult lastresult = stat?.LastResult;

                    // combine the results
                    if (lastresult != null)
                    {
                        int take = args.Where(a => a.StartsWith("/take:", StringComparison.InvariantCultureIgnoreCase)).Select(a => (int?)int.Parse(a.Substring(6))).FirstOrDefault() ?? 1;
                        int skip = args.Where(a => a.StartsWith("/skip:", StringComparison.InvariantCultureIgnoreCase)).Select(a => (int?)int.Parse(a.Substring(6))).FirstOrDefault() ?? 0;
                        var hist = stat.ExecutionHistory.Where(h => h.StartTime != stat.LastResult.StartTime);

                        hist = new[] { lastresult }.Union(hist).Skip(skip).Take(take);
                        lastresult = !hist.Any() ? null : new IndexerExecutionResult(
                                hist.First().Status,
                                string.Join(" | ", hist.Select(h => h.ErrorMessage)),
                                hist.Last().StartTime,
                                hist.First().EndTime,
                                hist.Reverse().SelectMany(h => h.Errors).ToList(),
                                hist.Reverse().SelectMany(h => h.Warnings).ToList(),
                                hist.Sum(h => h.ItemCount),
                                hist.Sum(h => h.FailedItemCount),
                                hist.Last().InitialTrackingState,
                                hist.First().FinalTrackingState
                        );
                    }

                    Console.WriteLine("{0,18} ({7}): {2,10} - Documents Processed: {3,-3}  Failed: {4,-3}  Warn({8}) Err({6}): {5}",
                        iName,
                        stat.Status,
                        lastresult?.Status,
                        lastresult?.ItemCount,
                        lastresult?.FailedItemCount,
                        lastresult?.ErrorMessage,
                        lastresult?.Errors?.Count,
                        ((lastresult?.EndTime ?? DateTimeOffset.UtcNow) - lastresult?.StartTime)?.ToString(@"hh\:mm\:ss"),
                        lastresult?.Warnings?.Count
                        );
                    if (args.Contains("errors", StringComparer.InvariantCultureIgnoreCase) || args.Contains("error", StringComparer.InvariantCultureIgnoreCase) || args.Contains("err", StringComparer.InvariantCultureIgnoreCase))
                    {
                        if (lastresult?.Errors?.Count > 0)
                        {
                            Console.WriteLine("Errors:");
                            foreach (var err in lastresult.Errors)
                            {
                                Console.WriteLine("      {0} - {1}",
                                    GetKeyValue(err.Key),
                                    err.ErrorMessage);
                            }
                        }
                    }
                    if (args.Contains("warnings", StringComparer.InvariantCultureIgnoreCase) || args.Contains("warning", StringComparer.InvariantCultureIgnoreCase) || args.Contains("warn", StringComparer.InvariantCultureIgnoreCase))
                    {
                        if (lastresult?.Warnings?.Count > 0)
                        {
                            Console.WriteLine("Warnings:");
                            foreach (var err in lastresult.Warnings)
                            {
                                Console.WriteLine("      {0} - {1}",
                                    GetKeyValue(err.Key),
                                    err.Message);
                            }
                        }
                    }
                }

                partitionStartValue = partitionEndValue;
            }


            if (args.Contains("import", StringComparer.InvariantCultureIgnoreCase))
            {
                await ImportData(instances);
            }
            
            return result;
        }


        private static string GetKeyValue(string key)
        {
            try
            {
                if (!string.IsNullOrWhiteSpace(key))
                    return Encoding.Unicode.GetString(Convert.FromBase64String(key));
            }
            catch
            {
            }
            return key;
        }

        private static async Task<bool> CreateIndexer(string name, string dsName, string description, bool disabled)
        {
            Console.WriteLine("Creating {1} Indexer {0}", name, disabled ? "Disabled" : "Enabled");
            try
            {
                string json = indexerTemplate;
                json = json.Replace("[IndexerName]", name);
                json = json.Replace("[DataSourceName]", dsName);
                json = json.Replace("[Disabled]", disabled.ToString().ToLowerInvariant());
                json = json.Replace("[BatchSize]", ConfigurationManager.AppSettings["BatchSize"]);
                json = json.Replace("[IndexName]", ConfigurationManager.AppSettings["IndexName"]);
                json = json.Replace("[Description]", description ?? "");
                string skipValidation = !disabled && bool.Parse(ConfigurationManager.AppSettings["SkipValidation"]) ? "&skipvalidation" : "";
                string uri = String.Format("{0}/indexers/{1}?api-version=2019-05-06{2}", _searchServiceEndpoint, name, skipValidation);
                HttpContent content = new StringContent(json, Encoding.UTF8, "application/json");
                HttpResponseMessage response = await _httpClient.PutAsync(uri, content);
                if (!response.IsSuccessStatusCode)
                {
                    string responseText = await response.Content.ReadAsStringAsync();
                    Console.WriteLine("Create Indexer response: \n{0}", responseText);
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error creating indexer: {0}", ex.Message);
                return false;
            }
            return true;
        }

        private static async Task<bool> CreateDataSource(string name, string description, object startValue, object endValue)
        {
            Console.WriteLine("Creating Datasource {0}", name);
            try
            {
                string json = datasourceTemplate;
                json = json.Replace("[DataSourceName]", name);
                json = json.Replace("[Description]", description ?? "");
                json = json.Replace("[ConnectionString]", ConfigurationManager.AppSettings["ConnectionString"]);
                json = json.Replace("[ContainerName]", ConfigurationManager.AppSettings["ContainerName"]);
                json = json.Replace("[ParitionStart]", startValue.ToString());
                json = json.Replace("[ParitionEnd]", endValue.ToString());
                string uri = String.Format("{0}/datasources/{1}?api-version=2019-05-06", _searchServiceEndpoint, name);
                HttpContent content = new StringContent(json, Encoding.UTF8, "application/json");
                HttpResponseMessage response = await _httpClient.PutAsync(uri, content);
                if (!response.IsSuccessStatusCode)
                {
                    string responseText = await response.Content.ReadAsStringAsync();
                    Console.WriteLine("Create Datasource response: \n{0}", responseText);
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error creating datasource: {0}", ex.Message);
                return false;
            }
            return true;
        }

        private static async Task<bool> ResetIndexer(string name)
        {
            Console.WriteLine("Reseting Indexer...");
            try
            {
                string uri = String.Format("{0}/indexers/{1}/reset?api-version=2019-05-06-Preview", _searchServiceEndpoint, name);
                HttpResponseMessage response = await _httpClient.PostAsync(uri, new StringContent(string.Empty));
                if (!response.IsSuccessStatusCode)
                {
                    string responseText = await response.Content.ReadAsStringAsync();
                    Console.WriteLine("Reset Indexer response: \n{0}", responseText);
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error creating indexer: {0}", ex.Message);
                return false;
            }
            return true;
        }

        private static async Task<bool> RunIndexer(string name)
        {
            Console.WriteLine("Runing Indexer...");
            try
            {
                string uri = String.Format("{0}/indexers/{1}/run?api-version=2019-05-06-Preview", _searchServiceEndpoint, name);
                HttpResponseMessage response = await _httpClient.PostAsync(uri, new StringContent(string.Empty));
                if (!response.IsSuccessStatusCode)
                {
                    string responseText = await response.Content.ReadAsStringAsync();
                    Console.WriteLine("Reset Indexer response: \n{0}", responseText);
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error creating indexer: {0}", ex.Message);
                return false;
            }
            return true;
        }

        private static async Task<IndexerExecutionInfo> GetIndexerStatus(string name)
        {
            return await _searchClient.Indexers.GetStatusAsync(name);
        }

        private static async Task<bool> DeleteObject(string name, string collection)
        {
            Console.WriteLine("Deleting {0} {1}", collection, name);
            try
            {
                string uri = String.Format("{0}/{1}/{2}?api-version=2019-05-06-Preview", _searchServiceEndpoint, collection, name);
                HttpResponseMessage response = await _httpClient.DeleteAsync(uri);

                if (!response.IsSuccessStatusCode && response.StatusCode != HttpStatusCode.NotFound)
                {
                    string responseText = await response.Content.ReadAsStringAsync();
                    Console.WriteLine("Reset Indexer response: \n{0}", responseText);
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error creating indexer: {0}", ex.Message);
                return false;
            }
            return true;
        }

        
        private static async Task ImportData(int degreeOfParallelism)
        {

            Console.WriteLine("Importing Data...");
            string fileName = ConfigurationManager.AppSettings["ImportFile"];
            string indexName = ConfigurationManager.AppSettings["IndexName"];
            int batchSize = int.Parse(ConfigurationManager.AppSettings["BatchSize"] ?? "1000");

            // open the file
            using (var stream = File.OpenRead(fileName))
            {
                List<Task> tasks = new List<Task>();

                // read  the stream into a blocking collection to synronize the single reader with the many writer
                var lines = ReadLines(stream);
                BlockingCollection<(string,int)[]> docs = new BlockingCollection<(string, int)[]>(degreeOfParallelism * 2);
                tasks.Add(Task.Run(() => {
                    List<(string, int)> batch = new List<(string, int)>(batchSize);
                    foreach (var line in lines.Select((line, lineNumber) => (line, lineNumber + 1)))
                    {
                        batch.Add(line);
                        if (batch.Count == batchSize)
                        {
                            docs.Add(batch.ToArray());
                            batch.Clear();
                        }
                    }
                    if (batch.Count > 0)
                        docs.Add(batch.ToArray());
                    docs.CompleteAdding();
                }));

                // Create the import tasks
                for (int i = 0; i < degreeOfParallelism; i++)
                {
                    int indexIndex = i; // local assignment needed for the async closure to work properly
                    tasks.Add(Task.Run(() => ImportIntoIndex(docs, indexName, indexIndex)));
                }

                // monitor the import and adjust the 
                Stopwatch timer = Stopwatch.StartNew();
                tasks.Add(Task.Run(async () => {
                    var runTimer = Stopwatch.StartNew();
                    while (!docs.IsCompleted)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(1));
                        Console.WriteLine("Elapsed: {0}  DocCount = {1}  AvgInsertMS = {2}  MDocsPerHour = {3:0.00}",
                            timer.Elapsed,
                            batchSize * _batchCounter,
                            _batchCounter > 0 ? Math.Round((double)_durationMS / _batchCounter) : 0,
                            _batchCounter * batchSize / 1000000.0 / (runTimer.Elapsed.TotalMinutes / 60)
                         );
                    }
                }));

                await Task.WhenAll(tasks);
            }
        }

        private static async Task ImportIntoIndex(BlockingCollection<(string, int)[]> batches, string indexName, int indexerIndex)
        {
            HttpClient httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Add("api-key", _apiKey);
            // For telemetry correlation purposes
            httpClient.DefaultRequestHeaders.Add("client-request-id", Guid.NewGuid().ToString());
            httpClient.DefaultRequestHeaders.UserAgent.Add(new System.Net.Http.Headers.ProductInfoHeaderValue("idxpart", "1.0"));

            int ramp = int.Parse(ConfigurationManager.AppSettings["RampMinutes"] ?? "0");

            // ramp indexer if desired
            await Task.Delay(TimeSpan.FromMinutes(ramp * indexerIndex));

            // In order to optimize for performance and isolate all other variables use the most low tech way to insert the data into the index as possible.
            // we just tweek the json to put it in a batch, add and id and action, and then post it to the rest api
            StringBuilder jsonPayload = new StringBuilder();
            while (!batches.IsCompleted)
            {
                var batch = batches.Take();
                if (batch == null)
                {
                    break;
                }

                try
                {
                    string uri = String.Format("{0}/indexes/{1}/docs/index?api-version=2019-05-06-Preview", _searchServiceEndpoint, indexName);

                    jsonPayload.Clear();
                    jsonPayload.Append("{\"value\":[");
                    bool isFirst = true;
                    foreach ((string line, int lineNumber) doc in batch)
                    {
                        if (isFirst)
                            isFirst = false;
                        else
                            jsonPayload.Append(",");

                        jsonPayload.Append("{\"@search.action\":\"mergeOrUpload\",\"" + (ConfigurationManager.AppSettings["KeyField"] ?? "id") + "\":\"");
                        jsonPayload.Append(doc.lineNumber);
                        jsonPayload.Append("\",");
                        jsonPayload.Append(doc.line.Substring(1, doc.line.Length - 1));
                    }
                    jsonPayload.Append("]}");

                    Stopwatch callTime = Stopwatch.StartNew();
                    HttpResponseMessage response = await httpClient.PostAsync(uri, new StringContent(jsonPayload.ToString(), Encoding.UTF8, "application/json"));
                    callTime.Stop();
                    Interlocked.Add(ref _durationMS, callTime.ElapsedMilliseconds);
                    if (!response.IsSuccessStatusCode)
                    {
                        string responseText = await response.Content.ReadAsStringAsync();
                        Console.WriteLine("Insert Index data response: \n{0}", responseText);
                    }
                    else
                    {
                        Interlocked.Increment(ref _batchCounter);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error inserting data into index: {0}", ex.Message);
                }
            }
        }

        private static IEnumerable<string> ReadLines(Stream s)
        {
            using (var reader = new StreamReader(s))
            {
                do
                {
                    while (reader.Peek() >= 0)
                    {
                        yield return reader.ReadLine();
                    }
                    s.Position = 0;
                }
                while (ConfigurationManager.AppSettings["Forever"] == "true");
            }
        }

    }
}
