using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace Funes {
    
    public class Reflection 
    {
        public struct Collision {
            public MemId Id { get; init; }
            public ReflectionId ReflectedVersion { get; init; }
            public ReflectionId LatestVersion { get; init; }
        }
        
        public ReflectionId Id { get; }
        
        public MemId[] NewFacts { get; }
        
        public (MemId, ReflectionId)[] InputKnowledge { get; }
        
        public MemId[] OutputKnowledge { get; }

        public Reflection(
            ReflectionId id, 
            MemId[] newFacts, 
            (MemId, ReflectionId)[] inputKnowledge, 
            MemId[] outputKnowledge) {
            Id = id;
            NewFacts = newFacts;
            InputKnowledge = inputKnowledge;
            OutputKnowledge = outputKnowledge;
        }

        public static async Task<ReflectionId> Reflect (
            IRepository repo,
            MemId[] newFacts,
            (MemId, ReflectionId)[] inputKnowledge,
            MemId[] outputKnowledge) {

            throw new NotImplementedException();
            // var reflectionId = ReflectionId.NewId();
            // var reflection = new Reflection(
            //     reflectionId, 
            //     newFacts.Select(mem => mem.Key).ToArray(), 
            //     inputKnowledge.Select(pair => (pair.Item1.Key, pair.Item2)).ToArray(),
            //     outputKnowledge.Select(mem => mem.Key).ToArray());
            //
            // var tasks =
            //     newFacts
            //         .Concat(outputKnowledge)
            //         .Append(await ReflectionToMem(reflection))
            //         .Select(mem => repo.PutMem(mem, reflectionId));
            //
            // await Task.WhenAll(tasks);
            //
            // return reflectionId;
        }

        public static async Task<(Reflection, Collision[])> Retrospect (
            IRepository repo, 
            ReflectionId reflectionId) {

            throw new NotImplementedException();

            // var mem = await repo.GetMem(Reflection.ReflectionKey, reflectionId);
            //
            // if (mem == null) {
            //     throw new Exception("Reflection not found: " + reflectionId.Id);
            // }
            //
            // var reflection = await Deserialize(mem.Content);
            //
            // if (reflection == null) {
            //     throw new Exception("Unable to deserialize reflection: " + reflectionId.Id);
            // }
            //
            // var historyTasks =
            //     reflection.InputKnowledge
            //         .Select(pair => repo.GetHistory(pair.Item1, reflectionId, 1));
            //
            // var historyItems = await Task.WhenAll(historyTasks);
            //
            // var collisions =
            //     reflection.InputKnowledge
            //         .Zip(historyItems,
            //         (pair, latestRid) => 
            //                         new Collision {
            //                             Key = pair.Item1,
            //                             ReflectedVersion = pair.Item2,
            //                             LatestVersion = latestRid.FirstOrDefault()
            //                         })
            //         .Where(c => String.CompareOrdinal(c.LatestVersion.Id, c.ReflectedVersion.Id) < 0)
            //     ;
            //
            //
            // return new (reflection, collisions.ToArray());
        }
        
        public static async Task<Stream> Serialize(Reflection reflection) {
            await using MemoryStream stream = new MemoryStream();
            await JsonSerializer.SerializeAsync(stream, reflection);
            return stream;
        }

        public static async Task<Reflection?> Deserialize(Stream content) {
            return await JsonSerializer.DeserializeAsync<Reflection>(content);
        }
        
        public static readonly MemId ReflectionId = new MemId("funes", "reflection");

        // public static async Task<Mem> ReflectionToMem(Reflection reflection) {
        //     return new Mem(ReflectionKey, null, await Serialize(reflection));
        // }
    }
}