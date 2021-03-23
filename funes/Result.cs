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
        public static Result<T> ReflectionError(Reflection reflection, Error[] errors) => 
            new Result<T>(new Error.ReflectionError(reflection, errors));
        public static Result<T> AggregateError(Error[] errors) => 
            new Result<T>(new Error.AggregateError(errors));
    }
    
    public abstract record Error {
        public record NoError : Error;
        public record MemNotFoundError : Error;
        public record ExceptionError(Exception Enn) : Error;
        public record NotSupportedEncodingError(string Encoding) : Error;
        public record SerdeError(string Msg) : Error;
        public record IoError(string Msg) : Error;
        public record ReflectionError(Reflection Reflection, Error[] Errors) : Error;
        public record AggregateError(Error[] Errors) : Error;

        public static readonly Error No = new NoError();
        public static readonly Error NotFound = new MemNotFoundError();
        public static readonly Error NotSupportedEncoding = new MemNotFoundError();
    }
 
}