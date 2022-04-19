using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace LargeTransaction
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("C# Transaction Test");

            if (args.Length == 0)
            {
                Console.WriteLine("Please enter a task to peform");
                return;
            }

            var task = args[0];

            /*
             * Set tasks here for easier testing.
             * 1. Set tasks[0] to add a group and assigned it to ~1M users
             * 2. Set tasks[1] to get a group's size
             * 3. Set tasks[2] to delete the group using a transaction. 
             * 4. Set tasks[3] to delete the group if using the transaction times out.
             */

            string[] tasks = new string[] { "AddGroup", "GroupSize", "DeleteGroupTxn", "DeleteGroup" };
            task = tasks[ 1 ];

            var databaseName = "test";
            var referenceGroup = "F";
            var newGroupId = "H";
            var newGroup = new BsonDocument { { "Id", "H" }, { "Label", "Hotel" } };
        

            Console.WriteLine($"Received argument: {task}");

            var MongoConnectString = Environment.GetEnvironmentVariable("MONGO_CONNECT_STRING");

            //https://mongodb.github.io/mongo-csharp-driver/2.15/getting_started/quick_tour
            var client = new MongoClient(MongoConnectString);
            var database = client.GetDatabase(databaseName);
            var usersCollection = database.GetCollection<BsonDocument>("users");
            var groupsCollection = database.GetCollection<BsonDocument>("groups");



            if (task == "GroupSize")
            {
                var group = newGroupId;
                var groupSize = GroupSize(usersCollection, group);
                Console.WriteLine($"Group {group} has {groupSize} members");
                return;
            }

            if (task =="AddGroup")
            {
                AddGroup(usersCollection, groupsCollection, newGroup, referenceGroup);
                return;
            }

            if (task == "DeleteGroup")
            {
                DeleteGroup(usersCollection, groupsCollection, newGroupId);
                return;
            }

            if (task == "DeleteGroupTxn")
            {
                DeleteGroupTxn(client, usersCollection, groupsCollection, newGroupId);
            }


        }

        static long GroupSize(IMongoCollection<BsonDocument> collection, string group)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("group.Id", group);
            var count = collection.CountDocuments(filter);
            return count;
        }

        static void AddGroup(IMongoCollection<BsonDocument> usersCollection, IMongoCollection<BsonDocument> groupsCollection, BsonDocument groupDocument, string referenceGroup)
        {
            Console.WriteLine($"Adding group {groupDocument}");

            // Add to groups collection
            groupsCollection.InsertOne(groupDocument);

            //  Using existing reference group as a cheap way to add a new group.
            var filter = Builders<BsonDocument>.Filter.Eq("group.Id", referenceGroup);
            var update = Builders<BsonDocument>.Update.Push("group", groupDocument);

            var result = usersCollection.UpdateMany(filter, update);

            if (result.IsModifiedCountAvailable)
            {
                Console.WriteLine($"{ result.ModifiedCount} user profiles updated.");
            }
        }

        static BsonDocument getGroupDocument(IMongoCollection<BsonDocument> groupsCollection, string group)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("Id", group);
            var simpleProjection = Builders<BsonDocument>.Projection.Exclude("_id");
            BsonDocument groupDoc = groupsCollection.Find(filter).Project(simpleProjection).First();
            return groupDoc;
        }

        static void DeleteGroup(IMongoCollection<BsonDocument> usersCollection, IMongoCollection<BsonDocument> groupsCollection, string group)
        {
            var groupSize = GroupSize(usersCollection, group);
            Console.WriteLine($"Deleting {groupSize} members from group {group}");

            var groupDoc = getGroupDocument(groupsCollection, group);
            Console.WriteLine(groupDoc);

            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();

            try
            {

                var filter = Builders<BsonDocument>.Filter.Eq("group.Id", group);
                var update = Builders<BsonDocument>.Update.Pull("group", groupDoc);

                var result = usersCollection.UpdateMany(filter, update);

                if (result.IsModifiedCountAvailable)
                {
                    Console.WriteLine($"{ result.ModifiedCount} user profiles updated.");
                }

                // Delete from groups collection
                filter = Builders<BsonDocument>.Filter.Eq("Id", group);
                groupsCollection.DeleteOne(filter);
                Console.WriteLine($"Group {group} deleted from groups collection");
            }
            catch (Exception exception)
            {
                Console.WriteLine($"Exception: {exception}");
            }

            watch.Stop();
            Console.WriteLine($"Execution Time: {watch.ElapsedMilliseconds} ms");

        }


        static void DeleteGroupTxn(MongoClient client, IMongoCollection<BsonDocument> usersCollection, IMongoCollection<BsonDocument> groupsCollection, string group)
        {

            var groupSize = GroupSize(usersCollection, group);
            Console.WriteLine($"Deleting {groupSize} members from group {group}");

            var groupDoc = getGroupDocument(groupsCollection, group);
            Console.WriteLine(groupDoc);

            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();

            /*           
             * Using the Transation Callback API
             * https://www.mongodb.com/docs/manual/core/transactions-in-applications/
             */

            try
            {
                // Step 1: Start a client session.
                using (var session = client.StartSession())
                {
                    // Step 2: Optional. Define options to use for the transaction.
                    var transactionOptions = new TransactionOptions(
                        readPreference: ReadPreference.Primary,
                        readConcern: ReadConcern.Local,
                        writeConcern: WriteConcern.WMajority);

                    // Step 3: Define the sequence of operations to perform inside the transactions
                    var cancellationToken = CancellationToken.None; // normally a real token would be used
                    var result = session.WithTransaction(
                        (s, ct) =>
                        {
                            var filter = Builders<BsonDocument>.Filter.Eq("group.Id", group);
                            var update = Builders<BsonDocument>.Update.Pull("group", groupDoc);
                            var result = usersCollection.UpdateMany(session, filter, update);

                            if (result.IsModifiedCountAvailable)
                            {
                                Console.WriteLine($"{ result.ModifiedCount} user profiles updated.");
                            }

                        // Delete from groups collection
                        filter = Builders<BsonDocument>.Filter.Eq("Id", group);
                            groupsCollection.DeleteOne(session, filter);
                            Console.WriteLine($"Group {group} deleted from groups collection");

                            return "$Group {group} deleted";

                        },
                        transactionOptions,
                        cancellationToken);
                }

            }
            catch (MongoDB.Driver.MongoCommandException commandException)
            {
                Console.WriteLine($"Exception: {commandException.Message}");
            }
            catch (Exception exception)
            {
                Console.WriteLine($"Exception: {exception}");
            }

            watch.Stop();
            Console.WriteLine($"Execution Time: {watch.ElapsedMilliseconds} ms");
        }

    }
}
