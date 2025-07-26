using Microsoft.Extensions.Caching.Memory;
using System.Reflection;

namespace NTDLS.SqlServerDapperWrapper
{
    /// <summary>
    /// Used to read EmbeddedResources from assemblies.
    /// </summary>
    internal static class EmbeddedResource
    {
        private static readonly MemoryCache _cache = new(new MemoryCacheOptions());

        /// <summary>
        /// Returns the given text, or if the script ends with ".sql", the script will be
        /// located and loaded form the executing assembly (assuming it is an embedded resource).
        /// </summary>
        public static string Load(string sqlTextOrEmbeddedResource)
        {
            string cacheKey = $":{sqlTextOrEmbeddedResource.ToLowerInvariant()}".Replace('.', ':').Replace('\\', ':').Replace('/', ':');

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

                throw new Exception($"The embedded script resource could not be found after enumeration: '{sqlTextOrEmbeddedResource}'");
            }
            return sqlTextOrEmbeddedResource;
        }

        /// <summary>
        /// Searches the given assembly for a script file.
        /// </summary>
        private static string? SearchAssembly(Assembly assembly, string scriptName)
        {
            var fileExtension = Path.GetExtension(scriptName);

            string cacheKey = scriptName;

            var allScriptNames = _cache.Get($"EmbeddedScripts:SearchAssembly:{assembly.FullName}") as List<string>;
            if (allScriptNames == null)
            {
                allScriptNames = assembly.GetManifestResourceNames().Where(o => o.EndsWith(fileExtension, StringComparison.InvariantCultureIgnoreCase))
                    .Select(o => $":{o}".Replace('.', ':')).ToList();
                _cache.Set("EmbeddedScripts:Names", allScriptNames, new MemoryCacheEntryOptions
                {
                    SlidingExpiration = new TimeSpan(1, 0, 0)
                });
            }

            if (allScriptNames.Count > 0)
            {
                var script = allScriptNames.Where(o => o.EndsWith(cacheKey, StringComparison.InvariantCultureIgnoreCase)).ToList();
                if (script.Count > 1)
                {
                    throw new Exception($"Ambiguous script name: [{cacheKey}].");
                }
                else if (script == null || script.Count == 0)
                {
                    return null;
                }

                using var stream = assembly.GetManifestResourceStream(script.Single().Replace(':', '.').Trim(new char[] { '.' }))
                    ?? throw new InvalidOperationException($"Script not found: [{cacheKey}].");

                using var reader = new StreamReader(stream);
                var scriptText = reader.ReadToEnd();

                _cache.Set(cacheKey, scriptText, new MemoryCacheEntryOptions
                {
                    SlidingExpiration = new TimeSpan(1, 0, 0)
                });

                return scriptText;
            }

            return null;
        }
    }
}
