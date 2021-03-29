using System;
using System.Collections.Generic;

namespace Funes {
    public abstract record Error {
        public record NoError : Error;
        public record MemNotFoundError : Error;
        public record ExceptionError(Exception Exn) : Error;
        public record NotSupportedEncodingError(string Encoding) : Error;
        public record SerdeError(string Msg) : Error;
        public record IoError(string Msg) : Error;
        public record CognitionError(Cognition Cognition, Error Error) : Error;

        public record CommitError(CommitError.FallaciousPremise[] FallaciousPremises) : Error {
            public readonly struct FallaciousPremise {
                public EntityId Eid { get; }
                public CognitionId PremiseCid { get; }
                public CognitionId ActualCid { get; }

                public override string ToString() => $"|{nameof(Eid)}: {Eid}, {nameof(PremiseCid)}: {PremiseCid}, {nameof(ActualCid)}: {ActualCid}|";
            }
        }
        public record AggregateError(params Error[] Errors) : Error;

        public static readonly Error No = new NoError();
        public static readonly Error NotFound = new MemNotFoundError();
    }
}