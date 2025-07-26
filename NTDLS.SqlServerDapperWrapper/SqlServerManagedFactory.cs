using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

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
        public SqlServerManagedFactory(IConfiguration configuration, string connectionStringName)
        {
            var connectionString = configuration.GetConnectionString(connectionStringName)
                ?? throw new Exception($"Connection string '{connectionStringName}' not found in configuration.");

            DefaultConnectionString = connectionString;
        }

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
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string sqlTextOrEmbeddedResource)
            => Ephemeral(DefaultConnectionString, o => o.Query<T>(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string sqlTextOrEmbeddedResource, object param)
            => Ephemeral(DefaultConnectionString, o => o.Query<T>(sqlTextOrEmbeddedResource, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public T QueryFirst<T>(string sqlTextOrEmbeddedResource)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirst<T>(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QueryFirstOrDefault<T>(string sqlTextOrEmbeddedResource, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirstOrDefault<T>(sqlTextOrEmbeddedResource)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T QueryFirst<T>(string sqlTextOrEmbeddedResource, object param)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirst<T>(sqlTextOrEmbeddedResource, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QueryFirstOrDefault<T>(string sqlTextOrEmbeddedResource, object param, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirstOrDefault<T>(sqlTextOrEmbeddedResource, param)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public T QuerySingle<T>(string sqlTextOrEmbeddedResource)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingle<T>(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T QuerySingle<T>(string sqlTextOrEmbeddedResource, object param)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingle<T>(sqlTextOrEmbeddedResource, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QuerySingleOrDefault<T>(string sqlTextOrEmbeddedResource, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingleOrDefault<T>(sqlTextOrEmbeddedResource, defaultValue));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T QuerySingleOrDefault<T>(string sqlTextOrEmbeddedResource, object param, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingleOrDefault<T>(sqlTextOrEmbeddedResource, param, defaultValue));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public T? QuerySingleOrDefault<T>(string sqlTextOrEmbeddedResource)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingleOrDefault<T>(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? QuerySingleOrDefault<T>(string sqlTextOrEmbeddedResource, object param)
            => Ephemeral(DefaultConnectionString, o => o.QuerySingleOrDefault<T>(sqlTextOrEmbeddedResource, param));


        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public T? QueryFirstOrDefault<T>(string sqlTextOrEmbeddedResource)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirstOrDefault<T>(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? QueryFirstOrDefault<T>(string sqlTextOrEmbeddedResource, object param)
            => Ephemeral(DefaultConnectionString, o => o.QueryFirstOrDefault<T>(sqlTextOrEmbeddedResource, param));

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        public void Execute(string sqlTextOrEmbeddedResource)
            => Ephemeral(DefaultConnectionString, o => o.Execute(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        public void Execute(string sqlTextOrEmbeddedResource, object param)
            => Ephemeral(DefaultConnectionString, o => o.Execute(sqlTextOrEmbeddedResource, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string sqlTextOrEmbeddedResource, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.ExecuteScalar<T>(sqlTextOrEmbeddedResource)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string sqlTextOrEmbeddedResource, object param, T defaultValue)
            => Ephemeral(DefaultConnectionString, o => o.ExecuteScalar<T>(sqlTextOrEmbeddedResource, param)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public T? ExecuteScalar<T>(string sqlTextOrEmbeddedResource)
            => Ephemeral(DefaultConnectionString, o => o.ExecuteScalar<T>(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public T? ExecuteScalar<T>(string sqlTextOrEmbeddedResource, object param)
            => Ephemeral(DefaultConnectionString, o => o.ExecuteScalar<T>(sqlTextOrEmbeddedResource, param));

        #endregion

        #region Query/Execute passthrough (async).

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(string sqlTextOrEmbeddedResource)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QueryAsync<T>(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(string sqlTextOrEmbeddedResource, object param)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QueryAsync<T>(sqlTextOrEmbeddedResource, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public async Task<T> QueryFirstAsync<T>(string sqlTextOrEmbeddedResource)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QueryFirstAsync<T>(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public async Task<T> QueryFirstOrDefaultAsync<T>(string sqlTextOrEmbeddedResource, T defaultValue)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QueryFirstOrDefaultAsync<T>(sqlTextOrEmbeddedResource)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public async Task<T> QueryFirstAsync<T>(string sqlTextOrEmbeddedResource, object param)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QueryFirstAsync<T>(sqlTextOrEmbeddedResource, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the first result or a default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public async Task<T> QueryFirstOrDefaultAsync<T>(string sqlTextOrEmbeddedResource, object param, T defaultValue)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QueryFirstOrDefaultAsync<T>(sqlTextOrEmbeddedResource, param)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public async Task<T> QuerySingleAsync<T>(string sqlTextOrEmbeddedResource)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QuerySingleAsync<T>(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public async Task<T> QuerySingleAsync<T>(string sqlTextOrEmbeddedResource, object param)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QuerySingleAsync<T>(sqlTextOrEmbeddedResource, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public async Task<T> QuerySingleOrDefaultAsync<T>(string sqlTextOrEmbeddedResource, T defaultValue)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QuerySingleOrDefaultAsync<T>(sqlTextOrEmbeddedResource, defaultValue));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public async Task<T> QuerySingleOrDefaultAsync<T>(string sqlTextOrEmbeddedResource, object param, T defaultValue)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QuerySingleOrDefaultAsync<T>(sqlTextOrEmbeddedResource, param, defaultValue));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public async Task<T?> QuerySingleOrDefaultAsync<T>(string sqlTextOrEmbeddedResource)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QuerySingleOrDefaultAsync<T>(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public async Task<T?> QuerySingleOrDefaultAsync<T>(string sqlTextOrEmbeddedResource, object param)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QuerySingleOrDefaultAsync<T>(sqlTextOrEmbeddedResource, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public async Task<T?> QueryFirstOrDefaultAsync<T>(string sqlTextOrEmbeddedResource)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QueryFirstOrDefaultAsync<T>(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns a single value or a default.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public async Task<T?> QueryFirstOrDefaultAsync<T>(string sqlTextOrEmbeddedResource, object param)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.QueryFirstOrDefaultAsync<T>(sqlTextOrEmbeddedResource, param));

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        public async Task ExecuteAsync(string sqlTextOrEmbeddedResource)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.ExecuteAsync(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Executes the given script name or SQL text on the database and does not return a result.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        public async Task ExecuteAsync(string sqlTextOrEmbeddedResource, object param)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.ExecuteAsync(sqlTextOrEmbeddedResource, param));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public async Task<T> ExecuteScalarAsync<T>(string sqlTextOrEmbeddedResource, T defaultValue)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.ExecuteScalarAsync<T>(sqlTextOrEmbeddedResource)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public async Task<T> ExecuteScalarAsync<T>(string sqlTextOrEmbeddedResource, object param, T defaultValue)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.ExecuteScalarAsync<T>(sqlTextOrEmbeddedResource, param)) ?? defaultValue;

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <returns></returns>
        public async Task<T?> ExecuteScalarAsync<T>(string sqlTextOrEmbeddedResource)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.ExecuteScalarAsync<T>(sqlTextOrEmbeddedResource));

        /// <summary>
        /// Queries the database using the given script name or SQL text and returns the scalar result or throws an exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlTextOrEmbeddedResource">tSQL text oe the name and path of an embedded resource file.</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public async Task<T?> ExecuteScalarAsync<T>(string sqlTextOrEmbeddedResource, object param)
            => await EphemeralAsync(DefaultConnectionString, async o => await o.ExecuteScalarAsync<T>(sqlTextOrEmbeddedResource, param));

        #endregion  
    }
}
