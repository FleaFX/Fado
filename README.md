# Fado

I needed a library that makes access to relational databases simple in F#. Here it is.

## Quick how to:

```F#
open Query

let db = DbContext.SqlServer.fromConnectionString "Data Source=.;Initial Catalog=Library;Integrated Security=True;"

// reading some data and mapping to a record
let books =
  "SELECT [Id], [Title]
  FROM [Books]
  WHERE [Author] = @Author AND [Isbn] = @Isbn"
  |> fromSql
  |> addParameter (Record({| Author = "Hugo Claus"; Isbn = "9789023430988" |})
  |> executeReader db
  |> fun row ->
      {
        Id = Map.uniqueidentifier "Id" row
        Title = Map.varchar "Title" row
      }
      
// performing data manipulation
"INSERT INTO [Books] ([Id], [Title])
VALUES (@Id, @Title)"
|> fromSql
|> addParameter (Record({| Id = (Guid.NewGuid()); Title = "The sorrow of Belgium" |}))
|> executeNonQuery db
```
