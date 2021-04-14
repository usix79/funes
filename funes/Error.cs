using System;
using System.Collections.Generic;
using System.Text;

namespace Funes {
    public abstract record Error {
        public record NoError : Error;
        public record NotFoundError : Error;
        public record ExceptionError(Exception Exn) : Error;
        public record NotSupportedEncodingError(string Encoding) : Error;
        public record SerdeError(string Msg) : Error;
        public record IoError(string Msg) : Error;
        public record IncrementError(Increment Increment, Error Error) : Error;
        public record MaxAttemptsError : Error;
        
        public record CommitError(CommitError.Conflict[] Conflicts) : Error {
            public readonly struct Conflict {
                public EntityId EntId { get; init; }
                public IncrementId PremiseIncId { get; init; }
                public IncrementId ActualIncId { get; init; }
                public override string ToString() => $"|{EntId}, Premise {PremiseIncId}, Actual {ActualIncId}|";
            }

            public override string ToString() {
                var txt = new StringBuilder("TransactionError: ");
                foreach (var cf in Conflicts) txt.Append(cf);
                return txt.ToString();
            }
        }

        public record AggregateError(IEnumerable<Error> Errors) : Error {
            public override string ToString() {
                var txt = new StringBuilder("AggregateError: ");
                foreach (var err in Errors) {
                    txt.Append(err).Append(',');
                }
                return txt.ToString();
            }
        }

        public static readonly Error No = new NoError();
        public static readonly Error NotFound = new NotFoundError();
        public static readonly Error MaxAttempts = new MaxAttemptsError();
    }
}