using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Funes.Impl;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Funes.Tests {
    public static class TestHelpers {
        
        private static readonly Random Rand = new Random(DateTime.Now.Millisecond);
        private static readonly ISerializer SimpleSerializer = new SimpleSerializer<Simple>();

        public static int RandomInt(int max) => Rand.Next(max);
        public static string RandomString(int length) =>
            string.Create(length, length, (span, n) => {
                for (var i = 0; i < n; i++) {
                    span[i] = (char) ('a' + Rand.Next(25));
                }
            });
        
        public static EntityId CreateRandomEntId(string? cat = null) =>
            new (cat ?? "cat-" + RandomString(10), "id-" + RandomString(10));

        public static IncrementId CreateRandomIncId(string? cat = null) => IncrementId.NewId();

        public static StampKey CreateRandomStampKey() => CreateRandomEntId().CreateStampKey(CreateRandomIncId());

        public static Simple CreateRandomValue() =>
            new (Rand.Next(1024), RandomString(1024));    
        
        public static BinaryStamp CreateSimpleStamp(IncrementId incId, EntityId? eid = null) {
            var entId = eid ??CreateRandomEntId();
            var encResult = SimpleSerializer.Encode(entId, CreateRandomValue());
            Assert.True(encResult.IsOk, encResult.Error.ToString());
            return new BinaryStamp(entId.CreateStampKey(incId), encResult.Value);
        }
        
        public static Event CreateEvent(IncrementId incId) =>
            new Event(incId, CreateRandomBuffer(256));

        public static byte[] CreateRandomBuffer(int size) {
            var arr = new byte[size];
            Rand.NextBytes(arr);
            return arr;
        } 

        public static void AssertEventsEqual(Event expected, Event actual) {
            Assert.Equal(expected.IncId, actual.IncId);
            Assert.Equal(expected.Data.Length, actual.Data.Length);
            var expectedSpan = expected.Data.Span;
            var actualSpan = expected.Data.Span;
            for (var i = 0; i < expected.Data.Length; i++) {
                Assert.Equal(expectedSpan[i], actualSpan[i]);
            }
        }

        public static void AssertEventsEqual(Event[] events, EventLog evtLog) {
            if (events.Length == 0) {
                Assert.Equal(0, evtLog.Memory.Length);
                Assert.Equal(IncrementId.None, evtLog.First);
                Assert.Equal(IncrementId.None, evtLog.Last);
                Assert.True(evtLog.IsEmpty);
                return;
            }
            
            Assert.Equal(events[0].IncId, evtLog.First);
            Assert.Equal(events[^1].IncId, evtLog.Last);

            var offset = 0;
            var actualSpan = evtLog.Memory.Span;
            foreach (var evt in events) {
                var expectedSpan = evt.Data.Span;
                for (var i = 0; i < evt.Data.Length; i++) {
                    Assert.Equal(expectedSpan[i], actualSpan[offset + i]);
                }

                offset += expectedSpan.Length;
            }
        }

        public static Entity CreateSimpleFact(int id, string value) =>
            new Entity(new EntityId("/tests/facts"), new Simple(id, value));

        public static StampKey[] Keys(params (EntityId, string)[] keys) =>
            Keys(keys.Select(pair => (pair.Item1, new IncrementId(pair.Item2))).ToArray());

        public static StampKey[] Keys(params (EntityId, IncrementId)[] keys) => 
            keys.Select(x => new StampKey(x.Item1, x.Item2)).ToArray();
        
        public static readonly StampKey[] EmptyKeys = Array.Empty<StampKey>(); 

        public static EntityId[] EntIds(params EntityId[] entIds) => entIds;

        public static bool AssertSequencesEqual<T>(IEnumerable<T> seq1, IEnumerable<T> seq2) {
            var idx = 0;
            var en1 = seq1.GetEnumerator();
            var en2 = seq2.GetEnumerator();
            while (en1.MoveNext()) {
                if (!en2.MoveNext()) return false;
                if (!en1.Current.Equals(en2.Current)) return false;
            }

            return !en2.MoveNext();
        }
    }
}