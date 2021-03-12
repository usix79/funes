using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Funes {
    
    public record Reflection (
        ReflectionId Id,
        IEnumerable<Mem> NewFacts,
        IEnumerable<(Mem, ReflectionId)> InputKnowledge,
        IEnumerable<Mem> OutputKnowledge
    ) {
        public static Task<ReflectionId> Reflect (
            IRepository repo,
            IEnumerable<Mem> newFacts,
            IEnumerable<(Mem, ReflectionId)> inputKnowledge,
            IEnumerable<Mem> outputKnowledge) {

            throw new NotImplementedException();
        }

        public static Task<bool> Retrospect (
            IRepository repo, 
            ReflectionId reflectionId) {
            
            throw new NotImplementedException();
        }
    }
}