using NTDLS.EmbeddedResource;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;

namespace NTDLS.SqlServerDapperWrapper
{
    /// <summary>
    /// Loads SQL scripts from embedded resources or file system.
    /// </summary>
    public static partial class SqlScriptLoader
    {
        /// <summary>
        /// Be a single word (no spaces or punctuation) or a bracketed expression like [name].
        /// </summary>
        [GeneratedRegex(@"^(\[?\w+\]?)(\.(\[?\w+\]?))*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
        private static partial Regex IsProcedureNameRegex();

        /// <summary>
        /// Loads a SQL script from an embedded resource or file system.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">The SQL text, stored procedure name or the name and path of an embedded resource file.</param>
        /// <returns>The loaded SQL script.</returns>
        public static string LoadSqlScript(string sqlTextOrEmbeddedResource)
        {
            if (sqlTextOrEmbeddedResource.EndsWith(".sql", StringComparison.InvariantCultureIgnoreCase))
            {
                sqlTextOrEmbeddedResource = EmbeddedResourceReader.LoadText(sqlTextOrEmbeddedResource);
            }
            return sqlTextOrEmbeddedResource;
        }

        /// <summary>
        /// Loads a SQL script from an embedded resource or file system.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">The SQL text, stored procedure name or the name and path of an embedded resource file.</param>
        /// <param name="param">An array of objects to format the text content of the embedded resource. The formatting is performed using string.Format semantics.</param>
        /// <param name="encoding"> Optional parameter to specify the encoding of the embedded resource. If not provided, UTF-8 encoding is used by default.</param>
        /// <returns>The loaded SQL script.</returns>
        public static string FormatSqlScript(string sqlTextOrEmbeddedResource, object[] param, Encoding? encoding = null)
        {
            if (sqlTextOrEmbeddedResource.EndsWith(".sql", StringComparison.InvariantCultureIgnoreCase))
            {
                sqlTextOrEmbeddedResource = EmbeddedResourceReader.Format(sqlTextOrEmbeddedResource, param, encoding);
            }
            return sqlTextOrEmbeddedResource;
        }

        /// <summary>
        /// Loads a SQL script from an embedded resource or file system.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource">The SQL text, stored procedure name or the name and path of an embedded resource file.</param>
        /// <param name="commandType">The command type determined based on the input string.</param>
        /// <returns>The loaded SQL script.</returns>
        public static string LoadSqlScript(string sqlTextOrEmbeddedResource, out CommandType? commandType)
        {
            if (sqlTextOrEmbeddedResource.EndsWith(".sql", StringComparison.InvariantCultureIgnoreCase))
            {
                sqlTextOrEmbeddedResource = EmbeddedResourceReader.LoadText(sqlTextOrEmbeddedResource);
            }
            commandType = GetCommandType(sqlTextOrEmbeddedResource);
            return sqlTextOrEmbeddedResource;
        }

        /// <summary>
        /// Determines the command type based on the input string.
        /// If it looks like a stored procedure name, returns CommandType.StoredProcedure; otherwise, returns CommandType.Text.
        /// </summary>
        /// <param name="sqlTextOrEmbeddedResource"></param>
        /// <returns></returns>
        public static CommandType GetCommandType(string sqlTextOrEmbeddedResource)
        {
            if (IsProcedureNameRegex().IsMatch(sqlTextOrEmbeddedResource.Trim()))
            {
                return CommandType.StoredProcedure;
            }

            return CommandType.Text;
        }
    }
}
