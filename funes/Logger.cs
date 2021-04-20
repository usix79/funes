using System;
using Microsoft.Extensions.Logging;

namespace Funes {
    
    public static class Logger {
        
        private const string ExceptionTemplate = "{Lib} {Subj}, {Kind} {IncId}";
        private const string ErrorTemplateWithIncId = "{Lib} {Subj}, {Kind} {IncId}, {Error}";
        private const string ErrorTemplate = "{Lib}, {Subj}, {Kind}, {Error}";
        private const string WarningTemplateWithIncId = "{Lib} {Subj}, {Kind} {IncId}";
        private const string WarningTemplate = "{Lib} {Subj}, {Kind} {Details}";
        private const string ErrorMessage = "{Lib} {Subj}, {Kind} {IncId}, {Error}";
        private const string DebugTemplate = "{Lib} {Subj}, {Kind} {Tag}";

        public static void FunesException(this ILogger logger, string subj, string kind, IncrementId incId, Exception x) {
            logger.LogError(x, ExceptionTemplate, "Funes", subj, kind, incId.Id);
        }

        public static void FunesError(this ILogger logger, string subj, string kind, IncrementId incId, object err) {
            logger.LogError(ErrorTemplateWithIncId, "Funes", subj, kind, incId.Id, err.ToString());
        }
        public static void FunesError(this ILogger logger, string subj, string kind, Error err) {
            logger.LogError(ErrorTemplate, "Funes", subj, kind, err.ToString());
        }
        public static void FunesErrorWarning(this ILogger logger, string subj, string kind, IncrementId incId, Error err) {
            logger.LogWarning(ErrorTemplateWithIncId, "Funes", subj, kind, incId.Id, err.ToString());
        }
        public static void FunesWarning(this ILogger logger, string subj, string kind, IncrementId incId) {
            logger.LogWarning(WarningTemplateWithIncId, "Funes", subj, kind, incId.Id);
        }
        public static void FunesWarning(this ILogger logger, string subj, string kind, string details) {
            logger.LogWarning(WarningTemplateWithIncId, "Funes", subj, kind, details);
        }

        public static void FunesDebug(this ILogger logger, string subj, string kind, string tag) {
            if (logger.IsEnabled(LogLevel.Debug)) 
                logger.LogDebug(DebugTemplate, "Funes", subj, kind, tag);
            
        }
    }
}