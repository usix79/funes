namespace Funes.Indexes {
    public enum PageKind {
        Unknown,
        Page = 1,   // contains value-key items
        Table = 2,  // contains first value - pageName items
        Catalog =3  // contains first value - tableName items 
    }
}