using System;
using System.Collections.Generic;

namespace Funes {
    public struct IncrementBuilder {
        private readonly EntityStampKey _factKey;
        private readonly long _startMilliseconds;
        private readonly int _attempt;
        private readonly long _startCommitMilliseconds;
        private readonly DateTimeOffset _incrementTime;
        private IncrementId _incId;
        private readonly List<KeyValuePair<string,string>> _details;
        private long _endCommitMilliseconds;
        private List<Error>? _errors;

        public IncrementId IncId => _incId;

        public IncrementBuilder(EntityStampKey factKey, long startMilliseconds, int attempt, long ms) {
            _incId = IncrementId.NewId();
            _factKey = factKey;
            _startMilliseconds = startMilliseconds;
            _attempt = attempt;
            _startCommitMilliseconds = ms;
            _endCommitMilliseconds = 0;
            _details = new List<KeyValuePair<string,string>>(8);
            _incrementTime = DateTimeOffset.UtcNow;
            _errors = null;
        }

        public void RegisterCommitResult(Result<Void> result, long ms) {
            _endCommitMilliseconds = ms;
            if (result.IsError) {
                if (result.Error is Error.CommitError err) {
                    _incId = IncId.AsFail();
                    AppendDetails(Increment.DetailsCommitErrors, err.ToString());
                }
                else {
                    _incId = _incId.AsLost();
                }
                _errors ??= new List<Error>();
                _errors.Add(result.Error);
            }
        }

        public void RegisterResult(Result<Void> result) {
            if (result.IsError) {
                _errors ??= new List<Error>();
                _errors.Add(result.Error);
            }
        }
        
        public void RegisterResults(ArraySegment<Result<Void>> results) {
            foreach (var result in results) {
                if (result.IsError) {
                    _errors ??= new List<Error>();
                    _errors.Add(result.Error);
                }
            }
        }

        public void RegisterResults(ArraySegment<Result<string>> results) {
            foreach (var result in results) {
                if (result.IsError) {
                    _errors ??= new List<Error>();
                    _errors.Add(result.Error);
                }
            }
        }

        public void DescribeSideEffects(string txt) => AppendDetails(Increment.DetailsSideEffects, txt);
        
        public Error GetError() {
            if (_errors is null) return Error.No;
            if (_errors!.Count == 1) return _errors[0];
            return new Error.AggregateError(_errors!);
        }

        public Increment Create(IncrementArgs args, List<EntityId> outputs, 
            List<KeyValuePair<string,string>> constants, long ms) {
            
            AppendDetails(Increment.DetailsIncrementTime, _incrementTime.ToString());
            AppendDetails(Increment.DetailsAttempt, _attempt.ToString());
            AppendDetails(Increment.DetailsLogicDuration, (_startCommitMilliseconds - _startMilliseconds).ToString()!);
            AppendDetails(Increment.DetailsCommitDuration, (_endCommitMilliseconds - _startCommitMilliseconds).ToString()!);
            AppendDetails(Increment.DetailsUploadDuration, (ms - _endCommitMilliseconds).ToString()!);

            if (_errors != null) {
                foreach (var error in _errors) AppendDetails(Increment.DetailsError, error.ToString());
            }

            return new Increment(_incId, _factKey, args, outputs, constants, _details);
        }

        private void AppendDetails(string key, string value) {
            _details.Add(new KeyValuePair<string, string>(key, value));
        }
    }
}
