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

        /// <summary>
        /// Returns the given text, or if the script ends with ".sql", the script will be
        /// located and loaded form the executing assembly (assuming it is an embedded resource).
        /// </summary>
        public static string TranslateSqlScript(string scriptNameOrText)
        {
            string cacheKey = $":{scriptNameOrText.ToLowerInvariant()}".Replace('.', ':').Replace('\\', ':').Replace('/', ':');

            if (cacheKey.EndsWith(":sql", StringComparison.InvariantCultureIgnoreCase))
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

                throw new Exception($"The embedded script resource could not be found after enumeration: '{scriptNameOrText}'");
            }

            return scriptNameOrText;
        }

        /// <summary>
        /// Searches the given assembly for a script file.
        /// </summary>
        private static string? SearchAssembly(Assembly assembly, string scriptName)
        {
            string cacheKey = scriptName;

            var allScriptNames = _cache.Get($"TranslateSqlScript:SearchAssembly:{assembly.FullName}") as List<string>;
            if (allScriptNames == null)
            {
                allScriptNames = assembly.GetManifestResourceNames().Where(o => o.EndsWith(".sql", StringComparison.InvariantCultureIgnoreCase))
                    .Select(o => $":{o}".Replace('.', ':')).ToList();
                _cache.Add("TranslateSqlScript:Names", allScriptNames, new CacheItemPolicy
                {
                    SlidingExpiration = new TimeSpan(1, 0, 0)
                });
            }

            if (allScriptNames.Count > 0)
            {
                var script = allScriptNames.Where(o => o.EndsWith(cacheKey, StringComparison.InvariantCultureIgnoreCase)).ToList();
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
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
            return NativeConnection.QuerySingleOrDefault<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        public void Execute(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
            NativeConnection.Execute(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        public void Execute(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
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
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
            return await NativeConnection.QuerySingleOrDefaultAsync<T>(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        public async Task ExecuteAsync(string sqlTextOrEmbeddedResource)
        {
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
            await NativeConnection.ExecuteAsync(sqlTextOrEmbeddedResource, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        public async Task ExecuteAsync(string sqlTextOrEmbeddedResource, object param)
        {
            sqlTextOrEmbeddedResource = TranslateSqlScript(sqlTextOrEmbeddedResource);
            await NativeConnection.ExecuteAsync(sqlTextOrEmbeddedResource, param, commandType: GetCommandType(sqlTextOrEmbeddedResource), transaction: GetCurrentTransaction());
        }

        #endregion
    }
}