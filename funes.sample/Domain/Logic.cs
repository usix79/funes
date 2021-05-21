using System.Collections.Generic;
using Funes.Indexes;

namespace funes.sample.Domain {
    using Funes;
    
    public class Logic : ILogic<Model, Message, SideEffect> {
        public (Model, Cmd<Message, SideEffect>) Begin(Entity fact, IConstants constants) =>
            fact.Value switch {
                Operation.PopulateSampleData => PopulateSampleData(),
                Operation.Like likeOp => BeginLikeOperation(likeOp.BookId),
                _ => (new Model(), Cmd<Message, SideEffect>.None)
            };

        public (Model, Cmd<Message, SideEffect>) Update(Model model, Message msg) =>
            msg switch {
                Message.LikeBook x => 
                    (model, new Cmd<Message, SideEffect>.UploadCmd(Helpers.CreateBookEntity(
                        new Book{Id = x.Book.Id, Name = x.Book.Name, Author = x.Book.Author, Likes = x.Book.Likes + 1}))),
                _ => (model, Cmd<Message, SideEffect>.None)
            };
        
        public Cmd<Message, SideEffect>.OutputCmd End(Model model) 
            => Cmd<Message, SideEffect>.None;
        
        private (Model, Cmd<Message, SideEffect>) PopulateSampleData() {
            var commands = new List<Cmd<Message, SideEffect>>();
            var id = 1;
            foreach(var fields in Helpers.GetBestBooks()) {
                var book = new Book {
                    Id = (id++).ToString(),
                    Name = fields[0],
                    Author = fields[1]
                };

                var entity = Helpers.CreateBookEntity(book);
                var cmd = new Cmd<Message, SideEffect>.UploadCmd(entity);
                commands.Add(cmd);
                var idxCmd = new Cmd<Message, SideEffect>.IndexCmd(
                    Helpers.Constants.IdxBooks, IndexOp.Kind.Update, book.Id, book.Name);
                commands.Add(idxCmd);
            }
            
            return (new Model(), new Cmd<Message, SideEffect>.BatchCmd(commands.ToArray()));
        }

        private (Model, Cmd<Message, SideEffect>) BeginLikeOperation(string bookId) {
            var cmd = new Cmd<Message, SideEffect>.RetrieveCmd(Helpers.CreateBookEntityId(bookId),
                entry => entry.IsOk ? new Message.LikeBook{Book =(Book)entry.Value} : new Message());
            return (new Model(), cmd);

        }
    }
}