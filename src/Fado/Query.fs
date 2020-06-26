namespace Fado

module Query = 

  open System
  open System.Data.Common
  open System.Reflection
  open Microsoft.FSharp.Reflection

  open DbContext

  let private tryReadValueProperty instance =
    match instance.GetType().GetProperty("Value") with
    | null -> None
    | _ as prop -> prop.GetGetMethod().Invoke(instance, [||]) |> Some

  let private isOptionValue value =
    if value = null then false else
    let typ = value.GetType()
    typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Option<_>>

  let private normalize value =
    match isOptionValue value with
    | false when (value = null || value.GetType() = typeof<DBNull> ) -> box DBNull.Value
    | false -> value
    | true -> 
        match tryReadValueProperty value with
        | Some(v) -> v
        | None -> box DBNull.Value

  type private RecordParameters = RecordParameters of PropertyInfo array
  with
    static member fromRecord obj = FSharpType.GetRecordFields(obj.GetType(), true) |> RecordParameters
    static member addParameters recordInstance (RecordParameters record) cmd =
      let addParameter c (propInfo : PropertyInfo) = c |> DbContext.addParameter (propInfo.Name, propInfo.GetValue(recordInstance) |> normalize)
      record |> Array.fold addParameter cmd

  type QueryParameter =
  | UniqueIdentifier of string * Guid
  | Varchar of string * string
  | Decimal of string * decimal
  | Tinyint of string * int16
  | Int of string * int32
  | Bigint of string * int64
  | Bit of string * bool
  | DateTime of string * DateTime
  | DateTimeOffset of string * DateTimeOffset
  | Record of obj
  
  type Query =
    private 
    | NonParametrised of string
    | Parametrised of string * QueryParameter list

  type QueryResult = private QueryResult of ResultSet

  /// <summary>
  /// Creates a new non parametrised <see cref="Query" /> from the given SQL expression.
  /// </summary>
  let fromSql = NonParametrised

  /// <summary>
  /// Adds the given <see cref="QueryParameter" /> to the given <see cref="Query" />.
  /// </summary>
  /// <param name="parameter">The <see cref="QueryParameter" /> to add.</param>
  let addParameter parameter = function
  | NonParametrised sql -> Parametrised(sql, [ parameter ])
  | Parametrised (sql, parameters) -> Parametrised(sql, parameter :: parameters)

  let private addParameterTuple = function
  | UniqueIdentifier (name, value) -> DbContext.addParameter (name, value)
  | Varchar (name, value) -> DbContext.addParameter (name, value)
  | Decimal (name, value) -> DbContext.addParameter (name, value)
  | Tinyint (name, value) -> DbContext.addParameter (name, value)
  | Int (name, value) -> DbContext.addParameter (name, value)
  | Bigint (name, value) -> DbContext.addParameter (name, value)
  | Bit (name, value) -> DbContext.addParameter (name, value)
  | DateTime (name, value) -> DbContext.addParameter (name, value)
  | DateTimeOffset (name, value) -> DbContext.addParameter (name, value)
  | Record record -> record |> RecordParameters.fromRecord |> RecordParameters.addParameters record

  let private asQueryResult (result : Async<ResultSet>) =
    async {
      let! result = result
      return QueryResult result
    }
 
  /// <summary>
  /// Executes the given <see cref="Query" /> in the given <see cref="DbContext" /> and returns a <see cref="QueryResult" />.
  /// </summary>
  /// <param name="context">The <see cref="DbContext" /> to execute the query in.</param>
  let executeQuery (context : DbContext) = function
  | NonParametrised sql ->
      context
      |> DbContext.prepareCommand sql
      |> DbContext.executeResultSet
      |> asQueryResult
  | Parametrised (sql, parameters) ->
      context
      |> DbContext.prepareCommand sql
      |> List.fold (fun cmd parameter -> cmd |> addParameterTuple parameter) <| parameters
      |> DbContext.executeResultSet
      |> asQueryResult

  /// <summary>
  /// Executes the given <see cref="Query" /> in the given <see cref="DbContext" /> as a non query.
  /// </summary>
  /// <param name="context">The <see cref="DbContext" /> to execute the query in .</param>
  let executeNonQuery (context : DbContext) = function
  | NonParametrised sql ->
      context
      |> DbContext.prepareCommand sql
      |> DbContext.executeNonQuery
  | Parametrised (sql, parameters) ->
      context
      |> DbContext.prepareCommand sql
      |> List.fold (fun cmd parameter -> cmd |> addParameterTuple parameter) <| parameters
      |> DbContext.executeNonQuery
 
  /// <summary>
  /// Executes the given <see cref="Query" /> in the given <see cref="DbContext" /> and returns a <see cref="DbDataReader" />.
  /// </summary>
  /// <param name="context">The <see cref="DbContext" /> to execute the query in.</param>
  let executeReader (context : DbContext) : Query -> Async<DbDataReader> = function
  | NonParametrised sql ->
      context
      |> DbContext.prepareCommand sql
      |> DbContext.executeReader
  | Parametrised (sql, parameters) ->
      context
      |> DbContext.prepareCommand sql
      |> List.fold (fun cmd parameter -> cmd |> addParameterTuple parameter) <| parameters
      |> DbContext.executeReader

  /// <summary>
  /// Maps each row in the given <see cref="QueryResult" /> using the given function.
  /// </summary>
  /// <param name="f">The mapping function.</param>
  let map context (map : DbDataReader -> 'a) (query : Query) =
    async {
      use! reader = (query |> executeReader context)
      use reader = new PeekDataReader(reader)
      return
        seq {
          while reader.Read() do
            yield reader |> map
        } |> Seq.toList
    }

  let splitOn colName (map : DbDataReader -> 'a) (reader : DbDataReader) =
    let reader = reader :?> PeekDataReader
    let cur = reader.GetValue(reader.GetOrdinal(colName))
    seq {
      yield reader |> map
      while reader.Peek() && cur = reader.GetValue(reader.GetOrdinal(colName)) do
        reader.Read() |> ignore
        yield reader |> map
    } |> Seq.toList

  module Map =

    let private fieldValue<'T> colName (row : DbDataReader) = row.GetFieldValue<'T>(row.GetOrdinal(colName))
  
    let uniqueidentifier = fieldValue<Guid>
    let varchar = fieldValue<string>
    let decimal = fieldValue<decimal>
    let tinyint = fieldValue<int16>
    let int = fieldValue<int32>
    let bigint = fieldValue<int64>
    let bit = fieldValue<bool>
    let datetime = fieldValue<DateTime>

    module Option = 
      let private fieldValue<'T> colName (row : DbDataReader) =
        let ordinal = row.GetOrdinal(colName)
        match row.IsDBNull(ordinal) with
        | false -> Some (row.GetFieldValue<'T>(row.GetOrdinal(colName)))
        | _ -> None
  
      let uniqueidentifier = fieldValue<Guid>
      let varchar = fieldValue<string>
      let decimal = fieldValue<decimal>
      let tinyint = fieldValue<int16>
      let int = fieldValue<int32>
      let bigint = fieldValue<int64>
      let bit = fieldValue<bool>
      let datetime = fieldValue<DateTime>
