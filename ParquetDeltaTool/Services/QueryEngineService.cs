using ParquetDeltaTool.Models;
using Microsoft.JSInterop;
using System.Text.RegularExpressions;

namespace ParquetDeltaTool.Services;

public class QueryEngineService : IQueryEngine
{
    private readonly IJSRuntime _jsRuntime;
    private readonly IStorageService _storage;
    private readonly ILogger<QueryEngineService> _logger;
    private readonly List<QueryResult> _queryHistory = new();

    public QueryEngineService(IJSRuntime jsRuntime, IStorageService storage, ILogger<QueryEngineService> logger)
    {
        _jsRuntime = jsRuntime;
        _storage = storage;
        _logger = logger;
    }

    public async Task<QueryResult> ExecuteQueryAsync(string sql, QueryOptions options)
    {
        var startTime = DateTime.UtcNow;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            // Validate query first
            var validation = await ValidateQueryAsync(sql);
            if (!validation.IsValid)
            {
                throw new InvalidOperationException($"Invalid query: {string.Join(", ", validation.Errors)}");
            }

            // For now, return mock data
            // TODO: Implement DuckDB WASM integration
            var result = await ExecuteMockQuery(sql, options);
            
            stopwatch.Stop();
            result.ExecutionTime = stopwatch.Elapsed;
            result.ExecutedAt = startTime;
            result.QueryText = sql;

            // Add to history
            _queryHistory.Add(result);
            if (_queryHistory.Count > 1000) // Keep only recent 1000 queries
            {
                _queryHistory.RemoveAt(0);
            }

            _logger.LogInformation("Executed query in {Duration}ms: {Query}", 
                stopwatch.ElapsedMilliseconds, sql.Length > 100 ? sql[..100] + "..." : sql);

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Query execution failed: {Query}", sql);
            
            var errorResult = new QueryResult
            {
                QueryText = sql,
                ExecutedAt = startTime,
                ExecutionTime = stopwatch.Elapsed,
                Rows = new List<Dictionary<string, object?>>()
            };
            
            throw;
        }
    }

    public async Task<ValidationResult> ValidateQueryAsync(string sql)
    {
        var result = new ValidationResult { IsValid = true };
        
        if (string.IsNullOrWhiteSpace(sql))
        {
            result.IsValid = false;
            result.Errors.Add("Query cannot be empty");
            return result;
        }

        // Basic SQL validation
        var trimmed = sql.Trim().ToUpperInvariant();
        
        // Only allow SELECT statements for security
        if (!trimmed.StartsWith("SELECT") && !trimmed.StartsWith("WITH"))
        {
            result.IsValid = false;
            result.Errors.Add("Only SELECT and WITH statements are allowed");
            return result;
        }

        // Check for dangerous keywords
        var dangerousKeywords = new[] { "DELETE", "UPDATE", "INSERT", "DROP", "CREATE", "ALTER", "EXEC", "EXECUTE" };
        foreach (var keyword in dangerousKeywords)
        {
            if (Regex.IsMatch(sql, $@"\b{keyword}\b", RegexOptions.IgnoreCase))
            {
                result.IsValid = false;
                result.Errors.Add($"Dangerous keyword '{keyword}' not allowed");
                return result;
            }
        }

        // Check for basic SQL syntax
        var parenthesesCount = sql.Count(c => c == '(') - sql.Count(c => c == ')');
        if (parenthesesCount != 0)
        {
            result.Warnings.Add("Unbalanced parentheses detected");
        }

        await Task.Delay(1); // Simulate async validation
        return result;
    }

    public async Task<List<QueryResult>> GetQueryHistoryAsync(Guid? fileId = null, int count = 50)
    {
        await Task.Delay(1); // Simulate async work
        
        var history = _queryHistory.AsEnumerable();
        
        // Filter by fileId if specified
        // TODO: Implement fileId filtering when we have proper file association
        
        return history
            .OrderByDescending(q => q.ExecutedAt)
            .Take(count)
            .ToList();
    }

    private async Task<QueryResult> ExecuteMockQuery(string sql, QueryOptions options)
    {
        // Simple mock implementation
        var maxRows = options.MaxRows ?? 1000;
        
        // Simulate different query types
        if (sql.ToLowerInvariant().Contains("count"))
        {
            return new QueryResult
            {
                Columns = new List<Column> { new() { Name = "count", DataType = "int64" } },
                Rows = new List<Dictionary<string, object?>>
                {
                    new() { ["count"] = Random.Shared.Next(1000, 10000) }
                },
                TotalRows = 1,
                ReturnedRows = 1,
                BytesScanned = 1024
            };
        }

        // Default: return sample data
        var result = new QueryResult
        {
            Columns = new List<Column>
            {
                new() { Name = "id", DataType = "int64" },
                new() { Name = "name", DataType = "string" },
                new() { Name = "value", DataType = "double" },
                new() { Name = "created_at", DataType = "timestamp" }
            }
        };

        var rowCount = Math.Min(maxRows, 100); // Limit mock data
        for (int i = 0; i < rowCount; i++)
        {
            result.Rows.Add(new Dictionary<string, object?>
            {
                ["id"] = i + 1,
                ["name"] = $"Sample Record {i + 1}",
                ["value"] = Math.Round(Random.Shared.NextDouble() * 1000, 2),
                ["created_at"] = DateTime.UtcNow.AddDays(-Random.Shared.Next(0, 365)).ToString("yyyy-MM-dd HH:mm:ss")
            });
        }

        result.TotalRows = rowCount;
        result.ReturnedRows = rowCount;
        result.BytesScanned = rowCount * 100; // Simulate bytes scanned

        // Simulate processing delay
        await Task.Delay(Random.Shared.Next(100, 500));

        return result;
    }
}