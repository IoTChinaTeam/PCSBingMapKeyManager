using Microsoft.Azure.Documents;

namespace PCSBingMapKeyManager
{
    internal class ThemeDocument : Resource
    {
        public string CollectionId { get; set; }
        public string Key { get; set; }
        public string Data { get; set; }
    }
}