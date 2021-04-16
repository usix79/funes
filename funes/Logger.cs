using System;
using Microsoft.Extensions.Logging;

namespace Funes {
    
    public static class Logger {
        
        private const string ExceptionTemplate = "{Lib} {Subj}, {Kind} {IncId}";
        private const string ErrorTemplate = "{Lib} {Subj}, {Kind} {IncId}, {Error}";
        private const string WarningTemplate = "{Lib} {Subj}, {Kind} {IncId}";
        private const string ErrorMessage = "{Lib} {Subj}, {Kind} {IncId}, {Error}";

        public static void FunesException(this ILogger logger, string subj, string kind, IncrementId incId, Exception x) {
            logger.LogError(x, ExceptionTemplate, "Funes", subj, kind, incId.Id);
        }

        public static void FunesError(this ILogger logger, string subj, string kind, IncrementId incId, Error err) {
            logger.LogError(ErrorTemplate, "Funes", subj, kind, incId.Id, err.ToString());
        }
        public static void FunesErrorMsg(this ILogger logger, string subj, string kind) {
            logger.LogError(ErrorTemplate, "Funes", subj, kind);
        }
        public static void FunesErrorWarning(this ILogger logger, string subj, string kind, IncrementId incId, Error err) {
            logger.LogWarning(ErrorTemplate, "Funes", subj, kind, incId.Id, err.ToString());
        }
        public static void FunesWarning(this ILogger logger, string subj, string kind, IncrementId incId) {
            logger.LogWarning(WarningTemplate, "Funes", subj, kind, incId.Id);
        }
    }
}