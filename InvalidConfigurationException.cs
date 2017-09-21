using System;

namespace PCSBingMapKeyManager
{
    internal class InvalidConfigurationException : Exception
    {
        public InvalidConfigurationException(string message) : base(message)
        {
        }
    }
}