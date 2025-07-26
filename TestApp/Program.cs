using NTDLS.SqlServerDapperWrapper;

namespace TestApp
{
    internal class Program
    {
        public static SqlServerManagedFactory MyConnection { get; set; } = new("localhost", "tempdb");

        static void Main()
        {
            //Create a test procedure.
            MyConnection.Execute("CREATE OR ALTER PROCEDURE TestProc AS\r\nBEGIN\r\n\tSELECT GetDate()\r\nEND\r\n");

            //Test procedure execution.
            var procValue = MyConnection.ExecuteScalar<DateTime>("TestProc");
            Console.WriteLine($"procValue: {procValue}");

            //Test tSQL execution.
            var textValue = MyConnection.ExecuteScalar<DateTime>("SELECT GetDate()");
            Console.WriteLine($"textValue: {textValue}");

            //Test embedded resource script execution.
            var sqlValue = MyConnection.ExecuteScalar<DateTime>("TestSqlFile.sql");
            Console.WriteLine($"sqlValue: {textValue}");

            //Each time a statement/query is executed, the NTDLS.SqlServerDapperWrapper will
            //  open a connection, execute then close & dispose the connection. 
            MyConnection.Execute("DROP TABLE IF EXISTS Test");

            //Creates a table in two different databases from a script that is an embedded resource in the project.
            MyConnection.Execute("CREATE TABLE Test( Id int identity(1,1) not null, [Name] varchar(128) NOT NULL, [Description] varchar(128) NULL)");

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

            //Just getting the results and writing them to the console.
            var results = MyConnection.Query<TestModel>("SELECT * FROM Test");
            //Print the results.
            foreach (var result in results)
            {
                Console.WriteLine($"{result.Id} {result.Name} {result.Description}");
            }

            Console.WriteLine("Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
