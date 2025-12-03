using Microsoft.Extensions.Caching.Memory;
using System.Reflection;

namespace NTDLS.SqlServerDapperWrapper
{
    /// <summary>
    /// Used to read EmbeddedResources from assemblies.
    /// </summary>
    public static class EmbeddedResource
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
                    var scriptText = SearchAssembly(assembly, cacheKey, sqlTextOrEmbeddedResource);
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
        private static string? SearchAssembly(Assembly assembly, string scriptCacheKey, string scriptName)
        {
            var assemblyCacheKey = $"EmbeddedScripts:SearchAssembly:{assembly.FullName}";

            var allScriptNames = _cache.Get(assemblyCacheKey) as List<string>;
            if (allScriptNames == null)
            {
                allScriptNames = assembly.GetManifestResourceNames().Where(o => o.EndsWith(".sql", StringComparison.InvariantCultureIgnoreCase))
                    .Select(o => $":{o}".Replace('.', ':')).ToList();
                _cache.Set(assemblyCacheKey, allScriptNames, new MemoryCacheEntryOptions
                {
                    SlidingExpiration = TimeSpan.FromHours(1)
                });
            }

            if (allScriptNames.Count > 0)
            {
                var script = allScriptNames.Where(o => o.EndsWith(scriptCacheKey, StringComparison.InvariantCultureIgnoreCase)).ToList();
                if (script.Count > 1)
                {
                    throw new Exception($"Ambiguous script name: [{scriptName}].");
                }
                else if (script.Count == 0)
                {
                    return null;
                }

                using var stream = assembly.GetManifestResourceStream(script.Single().Replace(':', '.').Trim(['.']))
                    ?? throw new InvalidOperationException($"Script not found: [{scriptName}].");

                using var reader = new StreamReader(stream);
                var scriptText = reader.ReadToEnd();

                _cache.Set(scriptCacheKey, scriptText, new MemoryCacheEntryOptions
                {
                    SlidingExpiration = TimeSpan.FromHours(1)
                });

                return scriptText;
            }

            return null;
        }
    }
}
