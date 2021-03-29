using System;

namespace Funes {
    public abstract record Error {
        public record NoError : Error;
        public record MemNotFoundError : Error;
        public record ExceptionError(Exception Exn) : Error;
        public record NotSupportedEncodingError(string Encoding) : Error;
        public record SerdeError(string Msg) : Error;
        public record IoError(string Msg) : Error;
        public record ReflectionError(Cognition Cognition, Error Error) : Error;
        public record AggregateError(params Error[] Errors) : Error;

        public static readonly Error No = new NoError();
        public static readonly Error NotFound = new MemNotFoundError();
    }
}