using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
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

        public static void Main1(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine(@"
USAGE:  idxpart

EXAMPLES:
    idxpart create                          
    idxpart create enable
    idxpart status
    idxpart disable 2 5-6
    idxpart enable /SkipValidation:true     
    idxpart create /DatasourceTemplate:datasource-incremental.json /PartitionCount:1

");
                return;
            }

            OverrideCongifWithArguments(args);

            string searchServiceName = ConfigurationManager.AppSettings["SearchServiceName"];
            string apiKey = ConfigurationManager.AppSettings["SearchServiceApiKey"];

            indexerTemplate = File.ReadAllText(ConfigurationManager.AppSettings["IndexerTemplate"]);
            datasourceTemplate = File.ReadAllText(ConfigurationManager.AppSettings["DatasourceTemplate"]);

            _searchClient = new SearchServiceClient(searchServiceName, new SearchCredentials(apiKey));

            _httpClient.DefaultRequestHeaders.Add("api-key", apiKey);
            
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

            DateTimeOffset partitionStartDate;
            DateTimeOffset partitionEndDate;
            if (!DateTimeOffset.TryParse(ConfigurationManager.AppSettings["PartitionStartDate"], out partitionStartDate))
            {
                partitionStartDate = DateTimeOffset.FromUnixTimeSeconds(long.Parse(ConfigurationManager.AppSettings["PartitionStartDate"]));
            }
            if (!DateTimeOffset.TryParse(ConfigurationManager.AppSettings["PartitionEndDate"], out partitionEndDate))
            {
                partitionEndDate = DateTimeOffset.FromUnixTimeSeconds(long.Parse(ConfigurationManager.AppSettings["PartitionEndDate"]));
            }

            TimeSpan interval = TimeSpan.FromTicks((long)((partitionEndDate - partitionStartDate).Ticks / (double)instances));
            
            DateTimeOffset partitionStartValue = partitionStartDate;
            DateTimeOffset partitionEndValue;
            for (int i = 1; i <= instances; i++)
            {
                if (instanceFilter.Any() && !instanceFilter.Contains(i))
                {
                    continue;
                }

                partitionEndValue = partitionStartValue + interval;
                string suffix = instances > 1 ? "-" + instances.ToString("000") + "-" + i.ToString("000") : string.Empty;
                string iName = ConfigurationManager.AppSettings["IndexerNamePrefix"] + suffix;
                string dsName = string.IsNullOrEmpty(ConfigurationManager.AppSettings["DatasourceNamePrefix"]) ? iName : ConfigurationManager.AppSettings["DatasourceNamePrefix"] + suffix;
                string description = string.Format("Partition {0} of {1} from {2} to {3} ({4}-{5})", i, instances, partitionStartValue, partitionEndValue, partitionStartValue.ToUnixTimeSeconds(), partitionEndValue.ToUnixTimeSeconds());

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
                    result = await CreateDataSource(dsName, description, partitionStartValue.ToUnixTimeSeconds(), partitionEndValue.ToUnixTimeSeconds());
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
                json = json.Replace("[IndexName]", ConfigurationManager.AppSettings["IndexName"]);
                json = json.Replace("[Description]", description ?? "");
                string skipValidation = !disabled && bool.Parse(ConfigurationManager.AppSettings["SkipValidation"]) ? "&skipvalidation" : "";
                string uri = String.Format("{0}/indexers/{1}?api-version=2017-11-11{2}", _searchServiceEndpoint, name, skipValidation);
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
                json = json.Replace("[CosmosConnectionString]", ConfigurationManager.AppSettings["CosmosConnectionString"]);
                json = json.Replace("[CosmosContainerName]", ConfigurationManager.AppSettings["CosmosContainerName"]);
                json = json.Replace("[ParitionStart]", startValue.ToString());
                json = json.Replace("[ParitionEnd]", endValue.ToString());
                string uri = String.Format("{0}/datasources/{1}?api-version=2017-11-11", _searchServiceEndpoint, name);
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
                string uri = String.Format("{0}/indexers/{1}/reset?api-version=2017-11-11-Preview", _searchServiceEndpoint, name);
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
                string uri = String.Format("{0}/indexers/{1}/run?api-version=2017-11-11-Preview", _searchServiceEndpoint, name);
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
                string uri = String.Format("{0}/{1}/{2}?api-version=2017-11-11-Preview", _searchServiceEndpoint, collection, name);
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
    }
}
