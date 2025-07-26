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
    public class SqlServerManagedInstance : IDisposable
    {
        /// <summary>
        /// Be a single word (no spaces or punctuation) or a bracketed expression like [name].
        /// </summary>
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
        public SqlServerManagedInstance(SqlConnectionStringBuilder builder)
        {
            NativeConnection = new SqlConnection(builder.ToString());
            NativeConnection.Open();
        }

        /// <summary>
        /// Creases a new instance of ManagedDataStorageInstance.
        /// </summary>
        public SqlServerManagedInstance(string connectionString)
        {
            NativeConnection = new SqlConnection(connectionString);
            NativeConnection.Open();
        }

        /// <summary>
        /// Creates a new instance of ManagedDataStorageInstance.
        /// </summary>
        public SqlServerManagedInstance(string serverName, string databaseName)
        {
            var connectionString = new SqlConnectionStringBuilder()
            {
                DataSource = serverName,
                InitialCatalog = databaseName,
                TrustServerCertificate = true,
                IntegratedSecurity = true,
            }.ToString();

            NativeConnection = new SqlConnection(connectionString);
            NativeConnection.Open();
        }

        /// <summary>
        /// Creates a new instance of ManagedDataStorageInstance.
        /// </summary>
        public SqlServerManagedInstance(string serverName, string databaseName, string username, string password)
        {
            var connectionString = new SqlConnectionStringBuilder()
            {
                DataSource = serverName,
                InitialCatalog = databaseName,
                TrustServerCertificate = true,
                IntegratedSecurity = false,
                UserID = username,
                Password = password
            }.ToString();

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

        private CommandType GetCommandType(string sqlTextOrEmbeddedResource)
        {
            if (_isProcedureNameRegex.IsMatch(sqlTextOrEmbeddedResource.Trim()))
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
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.Query<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.Query<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string sqlTextOrEmbeddedResource, T defaultValue)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.ExecuteScalar<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string sqlTextOrEmbeddedResource, object param, T defaultValue)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.ExecuteScalar<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public T QueryFirst<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.QueryFirst<T>(sqlTextOrEmbeddedResource);
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T QueryFirst<T>(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.QueryFirst<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QueryFirstOrDefault<T>(string sqlTextOrEmbeddedResource, T defaultValue)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.QueryFirstOrDefault<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QueryFirstOrDefault<T>(string sqlTextOrEmbeddedResource, object param, T defaultValue)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.QueryFirstOrDefault<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public T QuerySingle<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.QuerySingle<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T QuerySingle<T>(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.QuerySingle<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QuerySingleOrDefault<T>(string sqlTextOrEmbeddedResource, T defaultValue)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.QuerySingleOrDefault<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QuerySingleOrDefault<T>(string sqlTextOrEmbeddedResource, object param, T defaultValue)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.QuerySingleOrDefault<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// /// Queries the database using the given script name or SQL text and returns a scalar value throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public T? ExecuteScalar<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.ExecuteScalar<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// /// Queries the database using the given script name or SQL text and returns a scalar value throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? ExecuteScalar<T>(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.ExecuteScalar<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public T? QueryFirstOrDefault<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.QueryFirstOrDefault<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? QueryFirstOrDefault<T>(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.QueryFirstOrDefault<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public T? QuerySingleOrDefault<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.QuerySingleOrDefault<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? QuerySingleOrDefault<T>(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return NativeConnection.QuerySingleOrDefault<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        public void Execute(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            NativeConnection.Execute(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        public void Execute(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            NativeConnection.Execute(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        #endregion

        #region Query/Execute passthrough (async).

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QueryAsync<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QueryAsync<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public async Task<T> ExecuteScalarAsync<T>(string sqlTextOrEmbeddedResource, T defaultValue)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.ExecuteScalarAsync<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public async Task<T> ExecuteScalarAsync<T>(string sqlTextOrEmbeddedResource, object param, T defaultValue)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.ExecuteScalarAsync<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public async Task<T> QueryFirstAsync<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QueryFirstAsync<T>(sqlTextOrEmbeddedResource);
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public async Task<T> QueryFirstAsync<T>(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QueryFirstAsync<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public async Task<T> QueryFirstOrDefaultAsync<T>(string sqlTextOrEmbeddedResource, T defaultValue)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QueryFirstOrDefaultAsync<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public async Task<T> QueryFirstOrDefaultAsync<T>(string sqlTextOrEmbeddedResource, object param, T defaultValue)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QueryFirstOrDefaultAsync<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public async Task<T> QuerySingleAsync<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QuerySingleAsync<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public async Task<T> QuerySingleAsync<T>(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QuerySingleAsync<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public async Task<T> QuerySingleOrDefaultAsync<T>(string sqlTextOrEmbeddedResource, T defaultValue)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QuerySingleOrDefaultAsync<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public async Task<T> QuerySingleOrDefaultAsync<T>(string sqlTextOrEmbeddedResource, object param, T defaultValue)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QuerySingleOrDefaultAsync<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// /// Queries the database using the given script name or SQL text and returns a scalar value throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public async Task<T?> ExecuteScalarAsync<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.ExecuteScalarAsync<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// /// Queries the database using the given script name or SQL text and returns a scalar value throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public async Task<T?> ExecuteScalarAsync<T>(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.ExecuteScalarAsync<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public async Task<T?> QueryFirstOrDefaultAsync<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QueryFirstOrDefaultAsync<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public async Task<T?> QueryFirstOrDefaultAsync<T>(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QueryFirstOrDefaultAsync<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public async Task<T?> QuerySingleOrDefaultAsync<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QuerySingleOrDefaultAsync<T>(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public async Task<T?> QuerySingleOrDefaultAsync<T>(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            return await NativeConnection.QuerySingleOrDefaultAsync<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        public async Task ExecuteAsync(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            await NativeConnection.ExecuteAsync(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        public async Task ExecuteAsync(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = EmbeddedResource.Load(sqlTextOrEmbeddedResource);
            await NativeConnection.ExecuteAsync(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        #endregion
    }
}