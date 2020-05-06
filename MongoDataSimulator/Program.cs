using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MongoDataSimulator
{
    class Program
    {
        private static MongoClient destMongoClient;

        private static string destDbName = ConfigurationManager.AppSettings["dbName"];

        private static string destCollectionName = ConfigurationManager.AppSettings["collectionName"];

        private static int batchSize = Int32.Parse(ConfigurationManager.AppSettings["batchsize"]);
        private static int sleepInterval = Int32.Parse(ConfigurationManager.AppSettings["sleep-interval-in-milliseconds"]);
        private static long docsCount;
        private static IMongoDatabase destDatabase;

        private static IMongoCollection<BsonDocument> destDocStoreCollection;

        static void Main(string[] args)
        {
           var val= extractRetryDuration("Error=16500, RetryAfterMs=64");
            string destConnectionString =

                ConfigurationManager.AppSettings["conn"];

            MongoClientSettings destSettings = MongoClientSettings.FromUrl(

                new MongoUrl(destConnectionString)

            );

            destMongoClient = new MongoClient(destSettings);

            destDatabase = destMongoClient.GetDatabase(destDbName);

            destDocStoreCollection = destDatabase.GetCollection<BsonDocument>(destCollectionName);

            InsertSampleDocs(batchSize).Wait();

        }

        private static IEnumerable<BsonDocument> GetSampleDocuments(int batchSize)
        {
            ConcurrentBag<BsonDocument> bsonDocuments = new ConcurrentBag<BsonDocument>();
            string sampleBson = "{{'id':'{0}', 'doc-id':{1}}}";
            Parallel.For(0, batchSize, (i) =>
            {
                bsonDocuments.Add(
                    BsonDocument.Parse(string.Format(sampleBson, System.Guid.NewGuid().ToString(), i))
                    );
            });
            return bsonDocuments.ToList<BsonDocument>();
        }

        private static async Task InsertDocument(BsonDocument doc)

        {
            await destDocStoreCollection.InsertOneAsync(doc);
        }

        private static async Task InsertAllDocuments(IEnumerable<BsonDocument> docs)

        {
            var tasks = new List<Task>();
            for (int j = 0; j < docs.Count(); j++)
            {
                tasks.Add(InsertDocument(docs.ToList()[j]));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            docsCount = docsCount + docs.Count();
            Console.WriteLine("Total documents copied so far: {0}", docsCount);

        }
        private static int extractRetryDuration(String message)
        {
            Regex rx = new Regex("RetryAfterMs=([0-9]+)",
                                 RegexOptions.Compiled);

            MatchCollection matches = rx.Matches(message);


            int retryAfter = 0;

            if (matches.Count > 0)
            {

                retryAfter = Int32.Parse(matches[0].Groups[1].Value);

            }

            return retryAfter;
        }
            private static async Task InsertSampleDocs(int batchSize)

        {
            while (true)
            {
                IEnumerable<BsonDocument> batch = GetSampleDocuments(batchSize);
                await InsertAllDocuments(batch);
                System.Threading.Thread.Sleep(sleepInterval);
            }
        }

    }
}

