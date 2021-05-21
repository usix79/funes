using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata.Ecma335;
using Funes;

namespace funes.sample.Domain {
    public static class Helpers {
        public static class Constants {
            public const string CatBooks = "books";
            public const string CatOperations = "ops";
            public const string OperationPopulate = "pop";
            public const string OperationLike = "like";
            public const string IdxBooks = "idx_books";
        }

        public static Entity CreateOperationPopulate() =>
            new Entity(new EntityId(Constants.CatOperations, Constants.OperationPopulate),
                new Operation.PopulateSampleData());

        public static Entity CreateOperationLike(string bookId) =>
            new Entity(new EntityId(Constants.CatOperations, Constants.OperationLike),
                new Operation.Like{BookId = bookId});

        public static EntityId CreateBookEntityId(string bookId) =>
            new (Constants.CatBooks, bookId);

        public static Entity CreateBookEntity(Book book) =>
            new (CreateBookEntityId( book.Id), book);

        public static IEnumerable<string[]> GetBestBooks() {
            var assembly = Assembly.GetExecutingAssembly();
            var resourceName = "funes.sample.best.csv";

            using var stream = assembly.GetManifestResourceStream(resourceName);
            using var reader = new StreamReader(stream);
            string result = reader.ReadToEnd();
            return result
                .Split('\n')
                .Select(row => row.Split(','));
        }
    }
}