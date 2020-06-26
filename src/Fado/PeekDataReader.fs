namespace System.Data.Common

open System
open System.Data

type internal PeekDataReader(reader : DbDataReader) =
  inherit DbDataReader()

  let mutable _wasPeeked = false
  let mutable _lastResult = false

  member this.Peek () =
    if _wasPeeked then _lastResult
    else
      let result = (this :> IDataReader).Read()
      _wasPeeked <- true
      result
  override this.Read () =
    if (_wasPeeked) then
      _wasPeeked <- false
      _lastResult
    else
      _lastResult <- reader.Read()
      _lastResult
      
  override this.NextResult () =
    _wasPeeked <- false
    reader.NextResult()

  override this.Close(): unit = reader.Close()        
  override this.Depth: int = reader.Depth
  override this.FieldCount: int = reader.FieldCount
  override this.GetBoolean(i: int): bool = reader.GetBoolean(i)
  override this.GetByte(i: int): byte = reader.GetByte(i)
  override this.GetBytes(i: int, fieldOffset: int64, buffer: byte [], bufferoffset: int, length: int): int64 = reader.GetBytes(i, fieldOffset, buffer, bufferoffset, length)
  override this.GetChar(i: int): char = reader.GetChar(i)
  override this.GetChars(i: int, fieldoffset: int64, buffer: char [], bufferoffset: int, length: int): int64 = reader.GetChars(i, fieldoffset, buffer, bufferoffset, length)
  override this.GetDataTypeName(i: int): string = reader.GetDataTypeName(i)
  override this.GetDateTime(i: int): DateTime = reader.GetDateTime(i)
  override this.GetDecimal(i: int): decimal = reader.GetDecimal(i)
  override this.GetDouble(i: int): float = reader.GetDouble(i)
  override this.GetFieldType(i: int): Type = reader.GetFieldType(i)
  override this.GetFloat(i: int): float32 = reader.GetFloat(i)
  override this.GetGuid(i: int): Guid = reader.GetGuid(i)
  override this.GetInt16(i: int): int16 = reader.GetInt16(i)
  override this.GetInt32(i: int): int = reader.GetInt32(i)
  override this.GetInt64(i: int): int64 = reader.GetInt64(i)
  override this.GetName(i: int): string = reader.GetName(i)
  override this.GetOrdinal(name: string): int = reader.GetOrdinal(name)
  override this.GetSchemaTable(): DataTable = reader.GetSchemaTable()
  override this.GetString(i: int): string = reader.GetString(i)
  override this.GetValue(i: int): obj = reader.GetValue(i)
  override this.GetValues(values: obj []): int = reader.GetValues(values)
  override this.IsClosed: bool = reader.IsClosed
  override this.IsDBNull(i: int): bool = reader.IsDBNull(i)
  override this.Item with get (i: int): obj = reader.Item(i)
  override this.Item with get (name: string): obj = reader.Item(name)
  override this.RecordsAffected: int = reader.RecordsAffected
  override this.HasRows with get () : bool = reader.HasRows
  override this.GetEnumerator() : System.Collections.IEnumerator = reader.GetEnumerator()