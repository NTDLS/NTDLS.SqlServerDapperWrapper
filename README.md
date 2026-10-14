# NTDLS.SqlServerDapperWrapper

**NTDLS.SqlServerDapperWrapper** is a lightweight C# NuGet package that provides a simple, managed interface to SQL Server on top of [Dapper](https://github.com/DapperLib/Dapper). Every call you make opens a connection, executes your statement, and closes and disposes the connection for you — no connection bookkeeping required. It also transparently detects when you pass a stored procedure name (and runs it as `CommandType.StoredProcedure`), accepts raw t-SQL text, or loads `.sql` scripts embedded in your project as resources. All of this is available in both synchronous and asynchronous flavors.

📦 Be sure to check out the NuGet package: https://www.nuget.org/packages/NTDLS.SqlServerDapperWrapper

## When to use it

You'll want this package if any of the following sound familiar:

- **You're tired of boilerplate.** Opening, closing, and disposing `SqlConnection`s around every Dapper call is repetitive and easy to get wrong. This wrapper handles the lifecycle for you so you can focus on the query.
- **You need short-lived, fire-and-forget database access.** Scripts, maintenance jobs, CLI tools, tests, and small utilities often just need "run this one statement" without managing a long-lived connection pool or connection object.
- **You mix stored procedures, ad-hoc t-SQL, and script files.** Instead of choosing between `CommandType.StoredProcedure` and `CommandType.Text`, just pass the string — a bare procedure name like `TestProc` is detected automatically, while anything else is treated as t-SQL text.
- **You keep SQL in embedded resource files.** Pass the name of an embedded `.sql` file (e.g. `"Scripts/CreateTables.sql"`) and it will be loaded and executed for you.
- **You want transactions across multiple statements without holding a connection open yourself.** The `Ephemeral(...)` / `EphemeralAsync(...)` methods give you a single connection for the duration of a delegate, where you can begin a transaction, run several statements, and commit or roll back — all cleaned up automatically afterwards.

If you *do* want a long-lived process with a persistent connection, you can use the underlying `SqlServerManagedInstance` class directly instead of going through the factory.

## Usage examples

### Create a factory

```csharp
using NTDLS.SqlServerDapperWrapper;

// Build a factory from a server and database name (uses Windows integrated security).
public static SqlServerManagedFactory MyConnection { get; set; } = new("localhost", "tempdb");
```

Other constructors are available: from a full connection string, from `IConfiguration` (`new(configuration, "MyConnectionString")`), or with username/password and an optional command timeout.

### Execute a statement

```csharp
// Each call opens a connection, executes the statement, then closes & disposes the connection.
MyConnection.Execute("DROP TABLE IF EXISTS Test");

// Create a table using inline t-SQL text.
MyConnection.Execute("CREATE TABLE Test (Id int identity(1,1) NOT NULL, [Name] varchar(128) NOT NULL, [Description] varchar(128) NULL)");
```

### Stored procedures, t-SQL, and embedded scripts

The same method accepts three kinds of input, detected automatically:

```csharp
// 1. A stored procedure name is detected and executed as CommandType.StoredProcedure.
var procValue = MyConnection.ExecuteScalar<DateTime>("TestProc");

// 2. Raw t-SQL text is executed as CommandType.Text.
var textValue = MyConnection.ExecuteScalar<DateTime>("SELECT GetDate()");

// 3. An embedded .sql resource file is loaded by name and executed.
var sqlValue = MyConnection.ExecuteScalar<DateTime>("TestSqlFile.sql");
```

### Parameters

```csharp
// Insert records using an inline statement and parameters.
for (int i = 0; i < 100; i++)
{
    var param = new
    {
        Name = $"Name #{i}",
        Description = Guid.NewGuid().ToString()
    };

    // Anonymous types work directly as Dapper parameters.
    MyConnection.Execute("INSERT INTO Test (Name, Description) VALUES (@Name, @Description)", param);
}
```

### Multiple statements on one connection (transactions)

```csharp
// Ephemeral() gives the delegate a single open connection for its lifetime,
// then closes & disposes it when the delegate returns.
MyConnection.Ephemeral(o =>
{
    using var tx = o.BeginTransaction();

    try
    {
        for (int i = 0; i < 100; i++)
        {
            var param = new
            {
                Name = $"Name #{i}",
                Description = Guid.NewGuid().ToString()
            };

            o.Execute("INSERT INTO Test (Name, Description) VALUES (@Name, @Description)", param);
        }

        tx.Commit();
    }
    catch
    {
        tx.Rollback();
        throw;
    }
});
```

There are async equivalents (`EphemeralAsync`) and generic overloads that return a value from the delegate.

### Query results

```csharp
// Map query results to a POCO.
var results = MyConnection.Query<TestModel>("SELECT * FROM Test");

foreach (var result in results)
{
    Console.WriteLine($"{result.Id} {result.Name} {result.Description}");
}
```

Full Dapper-style coverage is provided: `Query`, `QueryFirst`, `QueryFirstOrDefault`, `QuerySingle`, `QuerySingleOrDefault`, `ExecuteScalar`, and `Execute` — each with parameter and optional `commandTimeout` overloads, plus matching `...Async` versions.

## License

[MIT](https://choosealicense.com/licenses/mit/)
