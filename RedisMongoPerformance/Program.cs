using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using MongoDB.Bson;
using MongoDB.Driver;
using StackExchange.Redis;

namespace RedisMongoPerformance
{
    class Program
    {
        static void Main(string[] args)
        {
            const string pathToData = @"D:\Github\Software Development\Database\Comparisons\data.csv";
            var lines = File.ReadAllLines(pathToData);
            var headers = lines[0].Split(',');

            #region redis

            var muxer = ConnectionMultiplexer.Connect("localhost");
            // Get a connection to database 1. You can leave this parameter blank or select another db to connect to.
            var conn = muxer.GetDatabase(1);

            var entries = new HashEntry[headers.Length - 2];

            var idx = 1;
            var values = lines[idx].Split(',');
            for (var j = 2; j < headers.Length; j++)
            {
                entries[j - 2] = new HashEntry(headers[j], values[j]);
            }

            const int iterations = 100_000;
            var stopwatch = Stopwatch.StartNew();
            Console.WriteLine("Inserting to Redis...");
            for (var i = 0; i < iterations; i++)
            {
                conn.HashSet($"{headers[0]}:{iterations}.{values[1]}", entries);
            }

            stopwatch.Stop();
            Console.WriteLine(
                $"Average insertions speed for Redis:  {(stopwatch.ElapsedMilliseconds / (double) iterations):N5}ms");

            #endregion

            stopwatch.Reset();

            #region mongoDB

            const string connectionString =
                @"mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false";
            var dbClient = new MongoClient(connectionString);
            var db = dbClient.GetDatabase("ecommerce");
            var collection = db.GetCollection<BsonDocument>("orders");
            var documents = new List<BsonDocument>();
            for (var i = 0; i < iterations; i++)
            {
                var document = new BsonDocument();
                for (var j = 0; j < headers.Length; j++)
                {
                    document.Add(new BsonElement(headers[j], values[j]));
                }
                documents.Add(document);
            }
            stopwatch.Reset();
            stopwatch.Start();
            Console.WriteLine("Inserting to MongoDB...");
            for (var i = 0; i < iterations; i++)
            {
                collection.InsertOne(documents[i]);
            }

            stopwatch.Stop();
            Console.WriteLine(
                $"Average insertion speed for MongoDB:  {(stopwatch.ElapsedMilliseconds / (double) iterations):N5}ms");

            #endregion

            // Query the entries to the DB
        }
    }
}