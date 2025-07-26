using Microsoft.Data.SqlClient;

namespace NTDLS.SqlServerDapperWrapper
{
    /// <summary>
    /// An instance that creates ManagedDataStorageInstances based off of the connection string stored in this class.
    /// </summary>
    public class SqlServerManagedFactory
    {
        /// <summary>
        /// The connection string that will be used by the factory, can be set using SetConnectionString().
        /// </summary>
        public string DefaultConnectionString { get; private set; } = string.Empty;

        /// <summary>
        /// Delegate used for ephemeral operations.
        /// </summary>
        public delegate void EphemeralProc(SqlServerManagedInstance connection);

        /// <summary>
        /// Delegate used for ephemeral operations.
        /// </summary>
        public delegate Task EphemeralAsyncProc(SqlServerManagedInstance connection);

        /// <summary>
        /// Delegate used for ephemeral operations.
        /// </summary>
        public delegate T EphemeralProc<T>(SqlServerManagedInstance connection);

        /// <summary>
        /// Delegate used for ephemeral operations.
        /// </summary>
        public delegate Task<T> EphemeralAsyncProc<T>(SqlServerManagedInstance connection);

        #region Constructors.

        /// <summary>
        /// Creates a new instance of ManagedDataStorageFactory.
        /// </summary>
        /// <param name="connectionString"></param>
        public SqlServerManagedFactory(string connectionString)
        {
            DefaultConnectionString = connectionString;
        }

        /// <summary>
        /// Creates a new instance of ManagedDataStorageFactory.
        /// </summary>
        public SqlServerManagedFactory(string serverName, string databaseName)
        {
            DefaultConnectionString = new SqlConnectionStringBuilder()
            {
                DataSource = serverName,
                InitialCatalog = databaseName,
                TrustServerCertificate = true,
                IntegratedSecurity = true,
            }.ToString();
        }

        /// <summary>
        /// Creates a new instance of ManagedDataStorageFactory.
        /// </summary>
        public SqlServerManagedFactory(string serverName, string databaseName, string username, string password)
        {
            DefaultConnectionString = new SqlConnectionStringBuilder()
            {
                DataSource = serverName,
                InitialCatalog = databaseName,
                TrustServerCertificate = true,
                IntegratedSecurity = false,
                UserID = username,
                Password = password
            }.ToString();
        }

        /// <summary>
        /// Creates a new instance of ManagedDataStorageFactory.
        /// </summary>
        public SqlServerManagedFactory()
        {
        }

        #endregion

        /// <summary>
        /// Sets the connection string that will be used by this factory.
        /// </summary>
        /// <param name="connectionString"></param>
        public void SetConnectionString(string? connectionString)
        {
            DefaultConnectionString = connectionString ?? string.Empty;
        }

        #region Ephemeral.

        /// <summary>
        /// Instantiates/opens a SQL connection using the default connection string, executes the given delegate and then closed/disposes the connection.
        /// </summary>
        /// <param name="func"></param>
        public void Ephemeral(EphemeralProc func)
        {
            using var connection = new SqlServerManagedInstance(DefaultConnectionString);
            func(connection);
        }

        /// <summary>
        /// Instantiates/opens a SQL connection using the default connection string, executes the given delegate and then closed/disposes the connection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="func"></param>
        /// <returns></returns>
        public T Ephemeral<T>(EphemeralProc<T> func)
        {
            using var connection = new SqlServerManagedInstance(DefaultConnectionString);
            return func(connection);
        }

        /// <summary>
        /// Instantiates/opens a SQL connection using the given connection string, executes the given delegate and then closed/disposes the connection.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="func"></param>
        public void Ephemeral(string connectionString, EphemeralProc func)
        {
            using var connection = new SqlServerManagedInstance(connectionString);
            func(connection);
        }

        /// <summary>
        /// Instantiates/opens a SQL connection using the given connection string, executes the given delegate and then closed/disposes the connection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionString"></param>
        /// <param name="func"></param>
        /// <returns></returns>
        public T Ephemeral<T>(string connectionString, EphemeralProc<T> func)
        {
            using var connection = new SqlServerManagedInstance(connectionString);
            return func(connection);
        }

        #endregion

        #region Ephemeral (async).

        /// <summary>
        /// Instantiates/opens a SQL connection using the default connection string, executes the given delegate and then closed/disposes the connection.
        /// </summary>
        /// <param name="func"></param>
        /// 
        public async Task EphemeralAsync(EphemeralAsyncProc func)
        {
            using var connection = new SqlServerManagedInstance(DefaultConnectionString);
            await func(connection);
        }

        /// <summary>
        /// Instantiates/opens a SQL connection using the default connection string, executes the given delegate and then closed/disposes the connection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="func"></param>
        /// <returns></returns>
        public async Task<T> EphemeralAsync<T>(EphemeralAsyncProc<T> func)
        {
            using var connection = new SqlServerManagedInstance(DefaultConnectionString);
            return await func(connection);
        }

        /// <summary>
        /// Instantiates/opens a SQL connection using the given connection string, executes the given delegate and then closed/disposes the connection.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="func"></param>
        public async Task EphemeralAsync(string connectionString, EphemeralAsyncProc func)
        {
            using var connection = new SqlServerManagedInstance(connectionString);
            await func(connection);
        }

        /// <summary>
        /// Instantiates/opens a SQL connection using the given connection string, executes the given delegate and then closed/disposes the connection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionString"></param>
        /// <param name="func"></param>
        /// <returns></returns>
        public async Task<T> EphemeralAsync<T>(string connectionString, EphemeralAsyncProc<T> func)
        {
            using var connection = new SqlServerManagedInstance(connectionString);
            return await func(connection);
        }


        #endregion

        #region Query/Execute passthrough.

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.Query<T>(scriptName));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.Query<T>(scriptName, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <returns></returns>
        public T QueryFirst<T>(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirst<T>(scriptName));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QueryFirstOrDefault<T>(string scriptName, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirstOrDefault<T>(scriptName)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T QueryFirst<T>(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirst<T>(scriptName, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QueryFirstOrDefault<T>(string scriptName, object param, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirstOrDefault<T>(scriptName, param)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <returns></returns>
        public T QuerySingle<T>(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingle<T>(scriptName));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T QuerySingle<T>(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingle<T>(scriptName, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QuerySingleOrDefault<T>(string scriptName, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingleOrDefault<T>(scriptName, defaultValue));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QuerySingleOrDefault<T>(string scriptName, object param, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingleOrDefault<T>(scriptName, param, defaultValue));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <returns></returns>
        public T? QuerySingleOrDefault<T>(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingleOrDefault<T>(scriptName));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? QuerySingleOrDefault<T>(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingleOrDefault<T>(scriptName, param));


        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <returns></returns>
        public T? QueryFirstOrDefault<T>(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirstOrDefault<T>(scriptName));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? QueryFirstOrDefault<T>(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirstOrDefault<T>(scriptName, param));

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="scriptName"></param>
        public void Execute(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.Execute(scriptName));

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        public void Execute(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.Execute(scriptName, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string scriptName, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.ExecuteScalar<T>(scriptName)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string scriptName, object param, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.ExecuteScalar<T>(scriptName, param)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <returns></returns>
        public T? ExecuteScalar<T>(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.ExecuteScalar<T>(scriptName));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? ExecuteScalar<T>(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.ExecuteScalar<T>(scriptName, param));

        #endregion

        #region Query/Execute passthrough (async).

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <returns></returns>
        public IEnumerable<T> QueryAsync<T>(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.Query<T>(scriptName));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public IEnumerable<T> QueryAsync<T>(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.Query<T>(scriptName, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <returns></returns>
        public T QueryFirstAsync<T>(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirst<T>(scriptName));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QueryFirstOrDefaultAsync<T>(string scriptName, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirstOrDefault<T>(scriptName)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T QueryFirstAsync<T>(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirst<T>(scriptName, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QueryFirstOrDefaultAsync<T>(string scriptName, object param, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirstOrDefault<T>(scriptName, param)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <returns></returns>
        public T QuerySingleAsync<T>(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingle<T>(scriptName));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T QuerySingleAsync<T>(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingle<T>(scriptName, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QuerySingleOrDefaultAsync<T>(string scriptName, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingleOrDefault<T>(scriptName, defaultValue));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QuerySingleOrDefaultAsync<T>(string scriptName, object param, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingleOrDefault<T>(scriptName, param, defaultValue));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <returns></returns>
        public T? QuerySingleOrDefaultAsync<T>(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingleOrDefault<T>(scriptName));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? QuerySingleOrDefaultAsync<T>(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingleOrDefault<T>(scriptName, param));


        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <returns></returns>
        public T? QueryFirstOrDefaultAsync<T>(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirstOrDefault<T>(scriptName));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? QueryFirstOrDefaultAsync<T>(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirstOrDefault<T>(scriptName, param));

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="scriptName"></param>
        public void ExecuteAsync(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.Execute(scriptName));

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        public void ExecuteAsync(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.Execute(scriptName, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T ExecuteScalarAsync<T>(string scriptName, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.ExecuteScalar<T>(scriptName)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T ExecuteScalarAsync<T>(string scriptName, object param, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.ExecuteScalar<T>(scriptName, param)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <returns></returns>
        public T? ExecuteScalarAsync<T>(string scriptName)
            => Ephemeral(DefaultConnectionString, o => o.ExecuteScalar<T>(scriptName));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scriptName"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? ExecuteScalarAsync<T>(string scriptName, object param)
            => Ephemeral(DefaultConnectionString, o => o.ExecuteScalar<T>(scriptName, param));

        #endregion  
    }
}
