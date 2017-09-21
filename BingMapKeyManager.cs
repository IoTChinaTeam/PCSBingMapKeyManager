using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace PCSBingMapKeyManager
{
    internal class BingMapKeyManager
    {
        private const string urlKey = "CosmosDb:url";
        private const string keyKey = "CosmosDb:key";
        private const string databaseIdKey = "CosmosDb:databaseId";
        private const string collectionIdKey = "CosmosDb:collectionId";
        private const string documentIdKey = "CosmosDb:documentId";
        private const string BingMapKeyKey = "BingMapKey";

        private readonly DocumentClient documentClient;
        private readonly string databaseId;
        private readonly string collectionId;
        private readonly string documentId;
        private readonly string logicCollectionId;
        private readonly string logicKey;
        private readonly string collectionLink;
        private readonly string documentLink;

        public BingMapKeyManager(IConfigurationRoot configuration)
        {
            var url = configuration.GetValue<string>(urlKey);
            var key = configuration.GetValue<string>(keyKey);
            databaseId = configuration.GetValue<string>(databaseIdKey);
            collectionId = configuration.GetValue<string>(collectionIdKey);
            documentId = configuration.GetValue<string>(documentIdKey);

            var parts = documentId.Split('.');
            if (parts.Length != 2)
            {
                throw new InvalidConfigurationException($"Incorrect configuration {documentIdKey} = {documentId}. It must be in form <collection>.<key>");
            }

            logicCollectionId = parts[0];
            logicKey = parts[1];

            collectionLink = $"/dbs/{databaseId}/colls/{collectionId}";
            documentLink = $"{collectionLink}/docs/{documentId}";

            documentClient = new DocumentClient(new Uri(url), key);
        }

        public async Task<string> GetAsync()
        {
            var root = await ReadDocumentAsync();

            return root?[BingMapKeyKey]?.ToString();
        }

        public async Task<bool> SetAsync(string key)
        {
            var root = await ReadDocumentAsync() ?? new JObject();

            root[BingMapKeyKey] = key;
            return await WriteDocumentAsync(root);
        }

        private async Task<JToken> ReadDocumentAsync()
        {
            try
            {
                var response = await documentClient.ReadDocumentAsync<ThemeDocument>(documentLink);
                return JsonConvert.DeserializeObject(response.Document.Data) as JToken;
            }
            catch
            {
                return null;
            }
        }

        private async Task<bool> WriteDocumentAsync(JToken root)
        {
            try
            {
                await documentClient.ReadDocumentCollectionAsync(collectionLink);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Failed to set the key (collection is not available): {ex.Message}");
                return false;
            }

            var document = new ThemeDocument
            {
                Id = $"{documentId}",
                CollectionId = logicCollectionId,
                Key = logicKey,
                Data = JsonConvert.SerializeObject(root)
            };

            try
            {
                await documentClient.UpsertDocumentAsync(collectionLink, document);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Failed to set the key: {ex.Message}");
                return false;
            }

            return true;
        }
    }
}