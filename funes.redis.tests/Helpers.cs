using System;

namespace Funes.Redis.Tests {
    public static class Helpers {
        private const string ConnectionStringEnvName = "FUNES_REDIS_TESTS_CS";
        
        public static string ResolveConnectionString() =>
            Environment.GetEnvironmentVariable(ConnectionStringEnvName) ?? "localhost";
        
    }
}