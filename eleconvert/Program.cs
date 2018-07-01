using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
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
                using (var batchQuery = new SqlCommand("select distinct id/1000 as batch from roads", dbconnection) { CommandTimeout = 0 })
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
                int batchesDone = 0;
                using (var httpClient = new HttpClient { BaseAddress = new Uri("https://api.open-elevation.com/"), Timeout = Timeout.InfiniteTimeSpan })
                {
                    foreach(var batch in batches)
                    {
                        try
                        {
                            var locations = new ConcurrentBag<Location>();
                            using (var latLongsQuery = new SqlCommand("select lat,lon from roads where (id/1000) = @batch", dbconnection) { CommandTimeout = 0 })
                            {
                                latLongsQuery.Parameters.AddWithValue("@batch", batch);
                                using (var latLongsReader = await latLongsQuery.ExecuteReaderAsync().ConfigureAwait(false))
                                {
                                    while (await latLongsReader.ReadAsync().ConfigureAwait(false))
                                    {
                                        var lat = await latLongsReader.GetFieldValueAsync<double>(latLongsReader.GetOrdinal("lat")).ConfigureAwait(false);
                                        var lon = await latLongsReader.GetFieldValueAsync<double>(latLongsReader.GetOrdinal("lon")).ConfigureAwait(false);
                                        locations.Add(new Location { Latitude = lat, Longitude = lon });
                                    }
                                }
                                var request = new Request { Locations = locations.ToArray() };
                                var jsonRequestString = JsonConvert.SerializeObject(request, new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() });
                                var httpResponse = await httpClient.PostAsync("api/v1/lookup", new StringContent(jsonRequestString, Encoding.UTF8, "application/json"));

                                httpResponse.EnsureSuccessStatusCode();
                                var responseString = await httpResponse.Content.ReadAsStringAsync().ConfigureAwait(false);
                                var responseData = JsonConvert.DeserializeObject<Response>(responseString);

                                lock(resultsFileLock)
                                {
                                    File.AppendAllLines(resultsFile, responseData.Results.Select(r => $"{r.Latitude},{r.Longitude},{r.Elevation}"));
                                    Console.WriteLine(Interlocked.Increment(ref batchesDone));
                                }
                            }
                        }
                        catch(Exception e)
                        {
                            File.AppendAllLines(failedBatchesFile, new[] { $"{batch}: {e.ToString()}", string.Empty });
                        }
                    }
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
        public double Latitude { get; set; }
        public double Longitude { get; set; }
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
