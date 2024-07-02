# NTDLS.SqlServerDapperWrapper

📦 Be sure to check out the NuGet package: https://www.nuget.org/packages/NTDLS.SqlServerDapperWrapper

Provides a simple interface to a SQL Server database with automatic cleanup and stored procedure detection.
All managed and wrapped in Dapper (hence the name).

>**Examples:**
Keep in mind that you can also use the **SqlServerManagedInstance** directly if you really want long lived processes.


```csharp
public static SqlServerManagedFactory MyConnection { get; set; } = new("localhost", "tempdb");
```

```csharp

//Each time a statement/query is executed, the NTDLS.SqlServerDapperWrapper will
//  open a connection, execute then close & dispose the connection. 
MyConnection.Execute("DROP TABLE IF EXISTS Test");

```
```csharp
//Creates a table in two different databases from a script that is an embedded resource in the project.
MyConnection.Execute("CREATE TABLE Test( Id int identity(1,1) not null, [Name] varchar(128) NOT NULL, [Description] varchar(128) NULL)");

```
```csharp
//Deletes the data from the table "Test".
MyConnection.Execute("DELETE FROM Test");

//Insert some records using an inline statement and parameters.
for (int i = 0; i < 100; i++)
{
    var param = new
    {
        Name = $"Name #{i}",
        Description = Guid.NewGuid().ToString()
    };

    MyConnection.Execute("INSERT INTO Test (Name, Description) VALUES (@Name, @Description)", param);
}

```
```csharp
//We can use "Ephemeral" to perform multiple steps on the same connection, such as here where we
//  begin a transaction, insert data and then optionally commit or rollback the transaction.
//  The connection is closed and disposed after Ephemeral() executes.
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
```csharp

//Just getting the results and writing them to the console.
var results = MyConnection.Query<TestModel>("SELECT * FROM Test");
//Print the results.
foreach (var result in results)
{
    Console.WriteLine($"{result.Id} {result.Name} {result.Description}");
}
}
```


## License
[MIT](https://choosealicense.com/licenses/mit/)
