using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace eleconvert
{
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            using (var dbconnection = new SqlConnection("Data Source=localhost;Integrated Security=SSPI;Initial Catalog=elevation;MultipleActiveResultSets=True"))
            {
                await dbconnection.OpenAsync().ConfigureAwait(false);
                var batches = new List<int>();
                using (var batchQuery = new SqlCommand("select distinct batch from roads", dbconnection))
                {
                    using (var batchReader = await batchQuery.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        while (await batchReader.ReadAsync().ConfigureAwait(false))
                        {
                            batches.Add(await batchReader.GetFieldValueAsync<int>(batchReader.GetOrdinal("batch")));
                        }
                    }
                }

                const string failedBatchesFile = "failedbatches.txt";
                const string resultsFile = "results.txt";
                if(File.Exists(failedBatchesFile))
                {
                    File.Delete(failedBatchesFile);
                }

                if(File.Exists(resultsFile))
                {
                    File.Delete(resultsFile);
                }

                var resultsFileLock = new object();
                //batches = batches.Take(1).ToList();
                using (var httpClient = new HttpClient { BaseAddress = new Uri("https://api.open-elevation.com/") })
                {
                    var tasks = batches.Select(async batch =>
                    {
                        try
                        {
                            var locations = new ConcurrentBag<Location>();
                            using (var latLongsQuery = new SqlCommand("select lat,lon from roads where batch = @batch", dbconnection))
                            {
                                latLongsQuery.Parameters.AddWithValue("@batch", batch);
                                using (var latLongsReader = await latLongsQuery.ExecuteReaderAsync().ConfigureAwait(false))
                                {
                                    while (await latLongsReader.ReadAsync().ConfigureAwait(false))
                                    {
                                        var lat = await latLongsReader.GetFieldValueAsync<string>(latLongsReader.GetOrdinal("lat")).ConfigureAwait(false);
                                        var lon = await latLongsReader.GetFieldValueAsync<string>(latLongsReader.GetOrdinal("lon")).ConfigureAwait(false);
                                        locations.Add(new Location { Latitude = lat, Longitude = lon });
                                    }
                                }
                                var request = new Request { Locations = locations.ToArray() };
                                var jsonRequestString = JsonConvert.SerializeObject(request, new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() })
                                    .Replace("\"", "")
                                    .Replace("longitude", "\"longitude\"")
                                    .Replace("latitude", "\"latitude\"")
                                    .Replace("locations", "\"locations\"");
                                var httpResponse = await httpClient.PostAsync("api/v1/lookup", new StringContent(jsonRequestString, Encoding.UTF8, "application/json"));

                                httpResponse.EnsureSuccessStatusCode();
                                var responseString = await httpResponse.Content.ReadAsStringAsync().ConfigureAwait(false);
                                var responseData = JsonConvert.DeserializeObject<Response>(responseString);

                                lock(resultsFileLock)
                                {
                                    File.AppendAllLines(resultsFile, responseData.Results.Select(r => $"{r.Latitude},{r.Longitude},{r.Elevation}"));
                                }
                                Console.WriteLine(".");
                            }
                        }
                        catch(Exception e)
                        {
                            File.AppendAllLines(failedBatchesFile, new[] { $"{batch}: {e.ToString()}", string.Empty });
                        }
                    }).ToArray();
                    Task.WaitAll(tasks);
                }

                Console.WriteLine();
            }
            return 0;
        }
    }

    public class Request
    {
        public Location[] Locations { get; set; }
    }

    public class Location
    {
        public string Latitude { get; set; }
        public string Longitude { get; set; }
    }

    public class Response
    {
        public Result[] Results { get; set; }
    }

    public class Result
    {
        public string Latitude { get; set; }
        public string Longitude { get; set; }
        public int Elevation { get; set; }
    }
}
