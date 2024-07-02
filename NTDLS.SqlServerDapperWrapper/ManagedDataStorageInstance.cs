using Dapper;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Text.RegularExpressions;

namespace NTDLS.SqlServerDapperWrapper
{
    /// <summary>
    /// A disposable database connection wrapper that functions as an ephemeral instance.
    /// One instance of this class is generally created per query.
    /// </summary>
    public class ManagedDataStorageInstance : IDisposable
    {
        private static readonly Regex _isProcedureNameRegex = new(@"^(\[.*\]|\w+)$", RegexOptions.IgnoreCase);
        private SqlTransaction? _transaction;
        private bool _disposed = false;

        /// <summary>
        /// The native underlying SQL Server connection.
        /// </summary>
        public SqlConnection NativeConnection { get; private set; }

        #region Constructors.

        /// <summary>
        /// Creases a new instance of ManagedDataStorageInstance.
        /// </summary>
        public ManagedDataStorageInstance(SqlConnectionStringBuilder builder)
        {
            NativeConnection = new SqlConnection(builder.ToString());
            NativeConnection.Open();
        }

        /// <summary>
        /// Creases a new instance of ManagedDataStorageInstance.
        /// </summary>
        public ManagedDataStorageInstance(string connectionString)
        {
            NativeConnection = new SqlConnection(connectionString);
            NativeConnection.Open();
        }

        #endregion

        /// <summary>
        /// Closes and disposes of the native SQL Server connection.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _disposed = true;
                if (disposing)
                {
                    try
                    {
                        if (NativeConnection != null && NativeConnection.State == ConnectionState.Open)
                        {
                            NativeConnection.Close();
                        }
                    }
                    catch
                    {
                    }
                    finally
                    {
                        NativeConnection.Close();
                    }
                }
            }
        }

        /// <summary>
        /// Closes and disposes of the native SQL Server connection.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private CommandType GetCommandType(string sqlText)
        {
            if (_isProcedureNameRegex.IsMatch(sqlText.Trim()))
            {
                return CommandType.StoredProcedure;
            }

            return CommandType.Text;
        }

        /// <summary>
        /// Returns the currently active transaction, if any.
        /// </summary>
        public SqlTransaction? GetCurrentTransaction()
        {
            if (_transaction != null && _transaction.Connection == null)
            {
                _transaction = null;
            }

            return _transaction;
        }

        /// <summary>
        /// Begins an atomic transaction.
        /// </summary>
        /// <returns></returns>
        public SqlTransaction BeginTransaction()
        {
            if (_transaction != null)
            {
                throw new Exception("Nested transactions are not supported.");
            }

            _transaction = NativeConnection.BeginTransaction();
            return _transaction;
        }

        /// <summary>
        /// Begins an atomic transaction.
        /// </summary>
        /// <param name="isolationLevel"></param>
        /// <returns></returns>
        public SqlTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            if (_transaction != null)
            {
                throw new Exception("Nested transactions are not supported.");
            }

            _transaction = NativeConnection.BeginTransaction(isolationLevel);
            return _transaction;
        }

        #region Query/Execute passthrough.

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string sqlText)
            => NativeConnection.Query<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string sqlText, object param)
            => NativeConnection.Query<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string sqlText, T defaultValue)
            => NativeConnection.ExecuteScalar<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction()) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string sqlText, object param, T defaultValue)
            => NativeConnection.ExecuteScalar<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction()) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <returns></returns>
        public T QueryFirst<T>(string sqlText)
            => NativeConnection.QueryFirst<T>(sqlText);

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T QueryFirst<T>(string sqlText, object param)
            => NativeConnection.QueryFirst<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QueryFirstOrDefault<T>(string sqlText, T defaultValue)
            => NativeConnection.QueryFirstOrDefault<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction()) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QueryFirstOrDefault<T>(string sqlText, object param, T defaultValue)
            => NativeConnection.QueryFirstOrDefault<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction()) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <returns></returns>
        public T QuerySingle<T>(string sqlText)
            => NativeConnection.QuerySingle<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T QuerySingle<T>(string sqlText, object param)
            => NativeConnection.QuerySingle<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QuerySingleOrDefault<T>(string sqlText, T defaultValue)
            => NativeConnection.QuerySingleOrDefault<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction()) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QuerySingleOrDefault<T>(string sqlText, object param, T defaultValue)
            => NativeConnection.QuerySingleOrDefault<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction()) ?? defaultValue;

        /// <summary>
        /// /// Queries the database using the given script name or SQL text and returns a scalar value throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <returns></returns>
        public T? ExecuteScalar<T>(string sqlText)
            => NativeConnection.ExecuteScalar<T>(sqlText);

        /// <summary>
        /// /// Queries the database using the given script name or SQL text and returns a scalar value throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? ExecuteScalar<T>(string sqlText, object param)
            => NativeConnection.ExecuteScalar<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <returns></returns>
        public T? QueryFirstOrDefault<T>(string sqlText)
            => NativeConnection.QueryFirstOrDefault<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? QueryFirstOrDefault<T>(string sqlText, object param)
            => NativeConnection.QueryFirstOrDefault<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <returns></returns>
        public T? QuerySingleOrDefault<T>(string sqlText)
            => NativeConnection.QuerySingleOrDefault<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? QuerySingleOrDefault<T>(string sqlText, object param)
            => NativeConnection.QuerySingleOrDefault<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlText"></param>
        public void Execute(string sqlText)
            => NativeConnection.Execute(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        public void Execute(string sqlText, object param)
            => NativeConnection.Execute(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());

        #endregion
    }
}