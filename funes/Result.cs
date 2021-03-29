using System;

namespace Funes {
    
    public readonly struct Result<T> {
        public T Value { get; }
        public Error Error { get; }
        public bool IsOk => Error == Error.No;
        public bool IsError => Error != Error.No;
        public Result(T val) => (Value, Error) = (val, Error.No);
        public Result(Error err) => (Value, Error) = (default!, err);

        public static Result<T> NotFound => new (Error.NotFound);
        public static Result<T> Exception(Exception exn) => new (new Error.ExceptionError(exn));
        public static Result<T> NotSupportedEncoding(string encoding) => new (new Error.NotSupportedEncodingError(encoding));
        public static Result<T> SerdeError(string msg) => new (new Error.SerdeError(msg));
        public static Result<T> IoError(string msg) => new (new Error.IoError(msg));
        public static Result<T> ReflectionError(Cognition cognition, Error error) => 
            new (new Error.CognitionError(cognition, error));
        public static Result<T> CommitError(Error.CommitError.FallaciousPremise[] fallaciousPremises) =>
            new Result<T>(new Error.CommitError(fallaciousPremises));
        public static Result<T> AggregateError(Error[] errors) => 
            new (new Error.AggregateError(errors));
    }
}