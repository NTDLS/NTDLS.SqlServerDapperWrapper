using Dapper;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Reflection;
using System.Runtime.Caching;
using System.Text.RegularExpressions;

namespace NTDLS.SqlServerDapperWrapper
{
    /// <summary>
    /// A disposable database connection wrapper that functions as an ephemeral instance.
    /// One instance of this class is generally created per query.
    /// </summary>
    public class SqlServerManagedInstance : IDisposable
    {
        private static readonly MemoryCache _cache = new("ManagedDataStorageInstance");
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

        /// <summary>
        /// Returns the given text, or if the script ends with ".sql", the script will be
        /// located and loaded form the executing assembly (assuming it is an embedded resource).
        /// </summary>
        /// <param name="script"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public static string TranslateSqlScript(string script)
        {
            string cacheKey = $":{script.ToLower()}".Replace('.', ':');

            if (cacheKey.EndsWith(":sql"))
            {
                if (_cache.Get(cacheKey) is string cachedScriptText)
                {
                    return cachedScriptText;
                }

                var assemblies = AppDomain.CurrentDomain.GetAssemblies();

                foreach (var assembly in assemblies)
                {
                    var scriptText = SearchAssembly(assembly, cacheKey);
                    if (scriptText != null)
                    {
                        return scriptText;
                    }
                }

                throw new Exception($"The embedded script resource could not be found after enumeration: '{cacheKey}'");
            }

            return script;
        }

        /// <summary>
        /// Searches the given assembly for a script file.
        /// </summary>
        /// <param name="assembly"></param>
        /// <param name="cacheKey"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        /// <exception cref="InvalidOperationException"></exception>
        private static string? SearchAssembly(Assembly assembly, string cacheKey)
        {
            var allScriptNames = _cache.Get($"TranslateSqlScript:SearchAssembly:{assembly.FullName}") as List<string>;
            if (allScriptNames == null)
            {
                allScriptNames = assembly.GetManifestResourceNames().Where(o => o.ToLower().EndsWith(".sql"))
                    .Select(o => $":{o}".Replace('.', ':')).ToList();
                _cache.Add("TranslateSqlScript:Names", allScriptNames, new CacheItemPolicy
                {
                    SlidingExpiration = new TimeSpan(1, 0, 0)
                });
            }

            if (allScriptNames.Count > 0)
            {
                var script = allScriptNames.Where(o => o.ToLower().EndsWith(cacheKey)).ToList();
                if (script.Count > 1)
                {
                    throw new Exception($"The script name is ambiguous: {cacheKey}.");
                }
                else if (script == null || script.Count == 0)
                {
                    return null;
                }

                using var stream = assembly.GetManifestResourceStream(script.Single().Replace(':', '.').Trim(new char[] { '.' }))
                    ?? throw new InvalidOperationException("Script not found: " + cacheKey);

                using var reader = new StreamReader(stream);
                var scriptText = reader.ReadToEnd();

                _cache.Add(cacheKey, allScriptNames, new CacheItemPolicy
                {
                    SlidingExpiration = new TimeSpan(1, 0, 0)
                });

                return scriptText;
            }

            return null;
        }

        #region Query/Execute passthrough.

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string sqlText)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.Query<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string sqlText, object param)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.Query<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string sqlText, T defaultValue)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.ExecuteScalar<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string sqlText, object param, T defaultValue)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.ExecuteScalar<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <returns></returns>
        public T QueryFirst<T>(string sqlText)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.QueryFirst<T>(sqlText);
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T QueryFirst<T>(string sqlText, object param)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.QueryFirst<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QueryFirstOrDefault<T>(string sqlText, T defaultValue)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.QueryFirstOrDefault<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QueryFirstOrDefault<T>(string sqlText, object param, T defaultValue)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.QueryFirstOrDefault<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <returns></returns>
        public T QuerySingle<T>(string sqlText)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.QuerySingle<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T QuerySingle<T>(string sqlText, object param)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.QuerySingle<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QuerySingleOrDefault<T>(string sqlText, T defaultValue)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.QuerySingleOrDefault<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QuerySingleOrDefault<T>(string sqlText, object param, T defaultValue)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.QuerySingleOrDefault<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction()) ?? defaultValue;
        }

        /// <summary>
        /// /// Queries the database using the given script name or SQL text and returns a scalar value throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <returns></returns>
        public T? ExecuteScalar<T>(string sqlText)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.ExecuteScalar<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// /// Queries the database using the given script name or SQL text and returns a scalar value throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? ExecuteScalar<T>(string sqlText, object param)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.ExecuteScalar<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <returns></returns>
        public T? QueryFirstOrDefault<T>(string sqlText)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.QueryFirstOrDefault<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? QueryFirstOrDefault<T>(string sqlText, object param)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.QueryFirstOrDefault<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <returns></returns>
        public T? QuerySingleOrDefault<T>(string sqlText)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.QuerySingleOrDefault<T>(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? QuerySingleOrDefault<T>(string sqlText, object param)
        {
            sqlText = TranslateSqlScript(sqlText);
            return NativeConnection.QuerySingleOrDefault<T>(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlText"></param>
        public void Execute(string sqlText)
        {
            sqlText = TranslateSqlScript(sqlText);
            NativeConnection.Execute(sqlText, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlText"></param>
        /// <param name="param"></param>
        public void Execute(string sqlText, object param)
        {
            sqlText = TranslateSqlScript(sqlText);
            NativeConnection.Execute(sqlText, param, commandType: GetCommandType(sqlText), transaction: GetCurrentTransaction());
        }

        #endregion
    }
}