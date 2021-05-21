namespace funes.sample.Domain {
    public class Operation {
        
        public class PopulateSampleData : Operation {
        }
        
        public class Like : Operation {
            public string BookId { get; set; }
        }
    }
}