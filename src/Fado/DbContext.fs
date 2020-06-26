namespace Fado

module DbContext =

  open System
  open System.Data
  open System.Data.Common
  open System.Collections.Generic

  type PreparedCommand = private PreparedCommand of DbCommand

  type ResultField = ResultField of string * obj | DBNull of string  
  
  type DbContext (connection : DbConnection) =
    interface IDisposable with
      member _.Dispose() =
        connection.Dispose()
    with
      member _.tryOpen () =
        async {
          match connection.State with
          | ConnectionState.Closed ->
              return! connection.OpenAsync() |> Async.AwaitTask
          | _ -> return ()
        }
      member this.prepareCommand sql =
        async {
          do! this.tryOpen()
          let cmd = connection.CreateCommand()
          cmd.CommandText <- sql
          cmd.CommandTimeout <- 0
          return PreparedCommand cmd
        }
      member _.map f = f connection

  type ResultRow(reader : DbDataReader) =
    interface IEnumerable<ResultField> with
      member _.GetEnumerator () : IEnumerator<ResultField> =
        (seq {
          for i in 0..reader.FieldCount-1 do
            if (reader.IsDBNull(i)) then yield DBNull (reader.GetName(i))
            else yield ResultField (reader.GetName(i), reader.GetValue(i))
        }).GetEnumerator()
      member this.GetEnumerator () : System.Collections.IEnumerator =
        (this :> IEnumerable<ResultField>).GetEnumerator() :> System.Collections.IEnumerator

  type ResultSet(reader : DbDataReader) =
    interface IEnumerable<ResultRow> with
      member _.GetEnumerator () : IEnumerator<ResultRow> =
        (seq {
          while (reader.Read()) do
            yield ResultRow reader
        }).GetEnumerator()
      member this.GetEnumerator () : System.Collections.IEnumerator =
        (this :> IEnumerable<ResultRow>).GetEnumerator() :> System.Collections.IEnumerator

  let prepareCommand sql (context : DbContext) = context.prepareCommand sql
  
  /// <summary>
  /// Adds a query parameter to the given <see cref="DbCommand" />
  /// </summary>
  /// <param name="name">The name of the query parameter.</param>
  /// <param name="value">The value of the query parameter.</param>
  /// <param name="cmd">The <see cref="DbCommand" /> to add the query parmeter to.</param>
  let addParameter (name, value) cmd =
    async {
      let! (PreparedCommand cmd) = cmd
      let parameter = cmd.CreateParameter()
      parameter.ParameterName <- name
      parameter.Value <- value
      cmd.Parameters.Add(parameter) |> ignore
      return PreparedCommand cmd
    }
  
  /// <summary>
  /// Executes the given <see cref="DbCommand" /> and returns a <see cref="ResultSet" />
  /// </summary>
  /// <param name="cmd">The <see cref="DbCommand" /> to execute.</param>
  let executeResultSet cmd =
    async {
      let! (PreparedCommand cmd) = cmd
      let! reader = cmd.ExecuteReaderAsync() |> Async.AwaitTask
      return ResultSet reader
    }
  
  /// <summary>
  /// Executes the given <see cref="DbCommand" /> as a non query (i.e. any non-select DML expression)
  /// </summary>
  /// <param name="cmd">The <see cref="DbCommand" /> to execute.</param>
  let executeNonQuery cmd =
    async {
      let! (PreparedCommand cmd) = cmd
      return! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask
    }
  
  /// <summary>
  /// Executes the given <see cref="DbCommand" /> and returns a <see cref="DbDataReader" />
  /// </summary>
  /// <param name="cmd">The <see cref="DbCommand" /> to execute.</param>
  let executeReader cmd =
    async {
      let! (PreparedCommand cmd) = cmd
      return! cmd.ExecuteReaderAsync() |> Async.AwaitTask
    }

  module SqlServer =
    open Microsoft.Data.SqlClient

    /// <summary>
    /// Creates a new <see cref="DbContext" /> from the given connection string.
    /// </summary>
    /// <param name="connectionString">The connection string to use.</param>
    let fromConnectionString connectionString = new DbContext(new SqlConnection(connectionString) :> DbConnection)
