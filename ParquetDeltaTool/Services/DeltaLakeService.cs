using ParquetDeltaTool.Models;
using Microsoft.JSInterop;
using System.Text.Json;

namespace ParquetDeltaTool.Services;

public class DeltaLakeService : IDeltaLakeService
{
    private readonly IJSRuntime _jsRuntime;
    private readonly IStorageService _storageService;
    private readonly ILogger<DeltaLakeService> _logger;

    public DeltaLakeService(IJSRuntime jsRuntime, IStorageService storageService, ILogger<DeltaLakeService> logger)
    {
        _jsRuntime = jsRuntime;
        _storageService = storageService;
        _logger = logger;
    }

    public async Task<DeltaTableMetadata> CreateDeltaTableAsync(string tableName, TableSchema schema, Dictionary<string, string>? properties = null)
    {
        _logger.LogInformation("Creating Delta table: {TableName}", tableName);
        
        var deltaTable = new DeltaTableMetadata
        {
            FileName = tableName,
            Format = FileFormat.Delta,
            Version = 0,
            SchemaString = JsonSerializer.Serialize(schema),
            Configuration = properties ?? new Dictionary<string, string>(),
            Protocol = new DeltaProtocol(),
            CreatedAt = DateTime.UtcNow
        };

        // Create initial commit
        var initialCommit = new DeltaCommitInfo
        {
            Version = 0,
            Timestamp = DateTime.UtcNow,
            Operation = "CREATE TABLE",
            OperationParameters = new Dictionary<string, object>
            {
                ["isManaged"] = "false",
                ["description"] = $"Created table {tableName}",
                ["properties"] = properties ?? new Dictionary<string, string>()
            },
            ClientVersion = "parquet-delta-tool-1.0.0",
            IsBlindAppend = true
        };

        deltaTable.CommitHistory.Add(initialCommit);

        // Store metadata
        await _storageService.StoreFileMetadataAsync(deltaTable);
        
        _logger.LogInformation("Created Delta table {TableName} with version {Version}", tableName, deltaTable.Version);
        return deltaTable;
    }

    public async Task<DeltaTableMetadata> GetDeltaTableMetadataAsync(Guid fileId)
    {
        var metadata = await _storageService.GetFileMetadataAsync(fileId);
        if (metadata is DeltaTableMetadata deltaMetadata)
        {
            return deltaMetadata;
        }

        // Convert regular metadata to Delta metadata if needed
        var deltaTable = new DeltaTableMetadata
        {
            FileId = metadata.FileId,
            FileName = metadata.FileName,
            FileSize = metadata.FileSize,
            Format = FileFormat.Delta,
            CreatedAt = metadata.CreatedAt,
            ModifiedAt = metadata.ModifiedAt,
            Version = 0
        };

        return deltaTable;
    }

    public async Task<List<DeltaCommitInfo>> GetCommitHistoryAsync(Guid fileId, int limit = 50)
    {
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        return deltaTable.CommitHistory
            .OrderByDescending(c => c.Version)
            .Take(limit)
            .ToList();
    }

    public async Task<FileMetadata> ReadTableAtVersionAsync(Guid fileId, long version)
    {
        _logger.LogInformation("Reading table {FileId} at version {Version}", fileId, version);
        
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        
        // For now, simulate reading at version by returning current metadata with version info
        var versionedMetadata = new FileMetadata
        {
            FileId = deltaTable.FileId,
            FileName = $"{deltaTable.FileName}@v{version}",
            FileSize = deltaTable.FileSize,
            Format = FileFormat.Delta,
            CreatedAt = deltaTable.CreatedAt,
            ModifiedAt = deltaTable.ModifiedAt,
            Properties = new Dictionary<string, string>
            {
                ["delta.version"] = version.ToString(),
                ["delta.timestamp"] = DateTime.UtcNow.ToString("O")
            }
        };

        return versionedMetadata;
    }

    public async Task<FileMetadata> ReadTableAtTimestampAsync(Guid fileId, DateTime timestamp)
    {
        _logger.LogInformation("Reading table {FileId} at timestamp {Timestamp}", fileId, timestamp);
        
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        
        // Find the version closest to the timestamp
        var targetCommit = deltaTable.CommitHistory
            .Where(c => c.Timestamp <= timestamp)
            .OrderByDescending(c => c.Timestamp)
            .FirstOrDefault();

        var version = targetCommit?.Version ?? 0;
        
        return await ReadTableAtVersionAsync(fileId, version);
    }

    public async Task<List<long>> GetAvailableVersionsAsync(Guid fileId)
    {
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        return deltaTable.CommitHistory
            .Select(c => c.Version)
            .OrderBy(v => v)
            .ToList();
    }

    public async Task<long> MergeIntoTableAsync(Guid targetFileId, Guid sourceFileId, string mergeCondition, Dictionary<string, string> updateExpressions)
    {
        _logger.LogInformation("Merging source {SourceId} into target {TargetId}", sourceFileId, targetFileId);
        
        var deltaTable = await GetDeltaTableMetadataAsync(targetFileId);
        var newVersion = deltaTable.Version + 1;

        // Create merge commit
        var mergeCommit = new DeltaCommitInfo
        {
            Version = newVersion,
            Timestamp = DateTime.UtcNow,
            Operation = "MERGE",
            OperationParameters = new Dictionary<string, object>
            {
                ["predicate"] = mergeCondition,
                ["matchedPredicates"] = updateExpressions,
                ["notMatchedPredicates"] = new Dictionary<string, string>()
            },
            OperationMetrics = new Dictionary<string, object>
            {
                ["numTargetRowsInserted"] = Random.Shared.Next(1, 1000),
                ["numTargetRowsUpdated"] = Random.Shared.Next(1, 1000),
                ["numTargetRowsDeleted"] = Random.Shared.Next(0, 100),
                ["numSourceRows"] = Random.Shared.Next(100, 2000)
            },
            ClientVersion = "parquet-delta-tool-1.0.0"
        };

        deltaTable.Version = newVersion;
        deltaTable.ModifiedAt = DateTime.UtcNow;
        deltaTable.CommitHistory.Add(mergeCommit);

        await _storageService.StoreFileMetadataAsync(deltaTable);

        var rowsAffected = (long)mergeCommit.OperationMetrics["numTargetRowsInserted"] + 
                          (long)mergeCommit.OperationMetrics["numTargetRowsUpdated"];
        
        return rowsAffected;
    }

    public async Task<long> UpdateTableAsync(Guid fileId, string whereCondition, Dictionary<string, string> updateExpressions)
    {
        _logger.LogInformation("Updating table {FileId} with condition: {Condition}", fileId, whereCondition);
        
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        var newVersion = deltaTable.Version + 1;

        var updateCommit = new DeltaCommitInfo
        {
            Version = newVersion,
            Timestamp = DateTime.UtcNow,
            Operation = "UPDATE",
            OperationParameters = new Dictionary<string, object>
            {
                ["predicate"] = whereCondition,
                ["set"] = updateExpressions
            },
            OperationMetrics = new Dictionary<string, object>
            {
                ["numUpdatedRows"] = Random.Shared.Next(1, 1000),
                ["numCopiedRows"] = Random.Shared.Next(0, 500)
            },
            ClientVersion = "parquet-delta-tool-1.0.0"
        };

        deltaTable.Version = newVersion;
        deltaTable.ModifiedAt = DateTime.UtcNow;
        deltaTable.CommitHistory.Add(updateCommit);

        await _storageService.StoreFileMetadataAsync(deltaTable);

        return (long)updateCommit.OperationMetrics["numUpdatedRows"];
    }

    public async Task<long> DeleteFromTableAsync(Guid fileId, string whereCondition)
    {
        _logger.LogInformation("Deleting from table {FileId} with condition: {Condition}", fileId, whereCondition);
        
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        var newVersion = deltaTable.Version + 1;

        var deleteCommit = new DeltaCommitInfo
        {
            Version = newVersion,
            Timestamp = DateTime.UtcNow,
            Operation = "DELETE",
            OperationParameters = new Dictionary<string, object>
            {
                ["predicate"] = whereCondition
            },
            OperationMetrics = new Dictionary<string, object>
            {
                ["numRemovedFiles"] = Random.Shared.Next(1, 10),
                ["numDeletedRows"] = Random.Shared.Next(1, 1000),
                ["numCopiedRows"] = Random.Shared.Next(0, 500)
            },
            ClientVersion = "parquet-delta-tool-1.0.0"
        };

        deltaTable.Version = newVersion;
        deltaTable.ModifiedAt = DateTime.UtcNow;
        deltaTable.CommitHistory.Add(deleteCommit);

        await _storageService.StoreFileMetadataAsync(deltaTable);

        return (long)deleteCommit.OperationMetrics["numDeletedRows"];
    }

    public async Task<long> InsertIntoTableAsync(Guid fileId, object[] rows)
    {
        _logger.LogInformation("Inserting {RowCount} rows into table {FileId}", rows.Length, fileId);
        
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        var newVersion = deltaTable.Version + 1;

        var insertCommit = new DeltaCommitInfo
        {
            Version = newVersion,
            Timestamp = DateTime.UtcNow,
            Operation = "WRITE",
            OperationParameters = new Dictionary<string, object>
            {
                ["mode"] = "Append",
                ["partitionBy"] = "[]"
            },
            OperationMetrics = new Dictionary<string, object>
            {
                ["numFiles"] = 1,
                ["numOutputRows"] = rows.Length,
                ["numOutputBytes"] = rows.Length * 100 // Estimate 100 bytes per row
            },
            ClientVersion = "parquet-delta-tool-1.0.0",
            IsBlindAppend = true
        };

        deltaTable.Version = newVersion;
        deltaTable.ModifiedAt = DateTime.UtcNow;
        deltaTable.RowCount += rows.Length;
        deltaTable.CommitHistory.Add(insertCommit);

        await _storageService.StoreFileMetadataAsync(deltaTable);

        return rows.Length;
    }

    public async Task<Dictionary<string, object>> OptimizeTableAsync(Guid fileId, List<string>? zOrderColumns = null)
    {
        _logger.LogInformation("Optimizing table {FileId}", fileId);
        
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        var newVersion = deltaTable.Version + 1;

        var optimizeCommit = new DeltaCommitInfo
        {
            Version = newVersion,
            Timestamp = DateTime.UtcNow,
            Operation = "OPTIMIZE",
            OperationParameters = new Dictionary<string, object>
            {
                ["predicate"] = "[]",
                ["zOrderBy"] = zOrderColumns ?? new List<string>()
            },
            OperationMetrics = new Dictionary<string, object>
            {
                ["numRemovedFiles"] = Random.Shared.Next(5, 20),
                ["numAddedFiles"] = Random.Shared.Next(1, 5),
                ["filesRemoved"] = new { min = 1024, max = 1024 * 1024, avg = 512 * 1024, totalFiles = Random.Shared.Next(5, 20) },
                ["filesAdded"] = new { min = 1024 * 1024, max = 128 * 1024 * 1024, avg = 64 * 1024 * 1024, totalFiles = Random.Shared.Next(1, 5) }
            },
            ClientVersion = "parquet-delta-tool-1.0.0"
        };

        deltaTable.Version = newVersion;
        deltaTable.ModifiedAt = DateTime.UtcNow;
        deltaTable.CommitHistory.Add(optimizeCommit);

        await _storageService.StoreFileMetadataAsync(deltaTable);

        return optimizeCommit.OperationMetrics;
    }

    public async Task<Dictionary<string, object>> VacuumTableAsync(Guid fileId, TimeSpan retentionPeriod)
    {
        _logger.LogInformation("Vacuuming table {FileId} with retention {Retention}", fileId, retentionPeriod);
        
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        
        var cutoffTime = DateTime.UtcNow - retentionPeriod;
        var removableFiles = Random.Shared.Next(0, 10);
        var reclaimedBytes = removableFiles * 1024 * 1024 * Random.Shared.Next(1, 100); // Random MB per file

        var vacuumResult = new Dictionary<string, object>
        {
            ["filesDeleted"] = removableFiles,
            ["bytesReclaimed"] = reclaimedBytes,
            ["retentionCheckEnabled"] = true,
            ["defaultRetentionMillis"] = (long)TimeSpan.FromDays(7).TotalMilliseconds,
            ["specifiedRetentionMillis"] = (long)retentionPeriod.TotalMilliseconds,
            ["minFileRetentionTimestamp"] = cutoffTime
        };

        return vacuumResult;
    }

    public async Task<Dictionary<string, object>> CompactFilesAsync(Guid fileId, long targetFileSizeBytes = 1024 * 1024 * 128)
    {
        _logger.LogInformation("Compacting files for table {FileId} with target size {TargetSize}", fileId, targetFileSizeBytes);
        
        return await OptimizeTableAsync(fileId); // Reuse optimize logic for compaction
    }

    public async Task<TableSchema> GetTableSchemaAsync(Guid fileId, long? version = null)
    {
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        
        if (!string.IsNullOrEmpty(deltaTable.SchemaString))
        {
            try
            {
                return JsonSerializer.Deserialize<TableSchema>(deltaTable.SchemaString) ?? new TableSchema();
            }
            catch (JsonException)
            {
                // Fall back to default schema if deserialization fails
            }
        }

        // Return a default schema
        return new TableSchema
        {
            Fields = new List<SchemaField>
            {
                new() { Name = "id", Type = "long", Nullable = false },
                new() { Name = "name", Type = "string", Nullable = true },
                new() { Name = "value", Type = "double", Nullable = true },
                new() { Name = "timestamp", Type = "timestamp", Nullable = true }
            }
        };
    }

    public async Task<bool> IsSchemaCompatibleAsync(TableSchema currentSchema, TableSchema newSchema)
    {
        // Simple compatibility check - in production, this would be more comprehensive
        var currentFields = currentSchema.Fields.ToDictionary(f => f.Name, f => f);
        
        foreach (var newField in newSchema.Fields)
        {
            if (currentFields.TryGetValue(newField.Name, out var currentField))
            {
                // Check if type is compatible (simplified)
                if (currentField.Type != newField.Type && !IsTypeCompatible(currentField.Type, newField.Type))
                {
                    return false;
                }
                
                // Check if nullable constraint is compatible
                if (currentField.Nullable && !newField.Nullable)
                {
                    return false;
                }
            }
        }
        
        return true;
    }

    public async Task<DeltaTableMetadata> EvolveSchemaAsync(Guid fileId, TableSchema newSchema, bool allowDataLoss = false)
    {
        _logger.LogInformation("Evolving schema for table {FileId}", fileId);
        
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        var currentSchema = await GetTableSchemaAsync(fileId);
        
        if (!allowDataLoss && !await IsSchemaCompatibleAsync(currentSchema, newSchema))
        {
            throw new InvalidOperationException("Schema evolution would result in data loss. Set allowDataLoss=true to proceed.");
        }

        var newVersion = deltaTable.Version + 1;

        var evolveCommit = new DeltaCommitInfo
        {
            Version = newVersion,
            Timestamp = DateTime.UtcNow,
            Operation = "SET TBLPROPERTIES",
            OperationParameters = new Dictionary<string, object>
            {
                ["properties"] = new Dictionary<string, string> { ["delta.autoOptimize.optimizeWrite"] = "true" }
            },
            ClientVersion = "parquet-delta-tool-1.0.0"
        };

        deltaTable.Version = newVersion;
        deltaTable.ModifiedAt = DateTime.UtcNow;
        deltaTable.SchemaString = JsonSerializer.Serialize(newSchema);
        deltaTable.CommitHistory.Add(evolveCommit);

        await _storageService.StoreFileMetadataAsync(deltaTable);

        return deltaTable;
    }

    public async Task<List<DeltaAction>> GetTransactionLogAsync(Guid fileId, long? startVersion = null, long? endVersion = null)
    {
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        
        var actions = new List<DeltaAction>();
        
        foreach (var commit in deltaTable.CommitHistory)
        {
            if (startVersion.HasValue && commit.Version < startVersion) continue;
            if (endVersion.HasValue && commit.Version > endVersion) continue;
            
            actions.Add(new DeltaAction
            {
                Action = commit.Operation,
                Timestamp = commit.Timestamp,
                Data = new Dictionary<string, object>
                {
                    ["version"] = commit.Version,
                    ["operation"] = commit.Operation,
                    ["operationParameters"] = commit.OperationParameters,
                    ["operationMetrics"] = commit.OperationMetrics
                }
            });
        }
        
        return actions.OrderBy(a => a.Timestamp).ToList();
    }

    public async Task<long> CreateCheckpointAsync(Guid fileId)
    {
        _logger.LogInformation("Creating checkpoint for table {FileId}", fileId);
        
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        
        // Simulate checkpoint creation
        var checkpointVersion = deltaTable.Version;
        
        _logger.LogInformation("Created checkpoint at version {Version} for table {FileId}", checkpointVersion, fileId);
        
        return checkpointVersion;
    }

    public async Task<bool> ValidateTableIntegrityAsync(Guid fileId)
    {
        _logger.LogInformation("Validating integrity for table {FileId}", fileId);
        
        var deltaTable = await GetDeltaTableMetadataAsync(fileId);
        
        // Basic validation checks
        var isValid = !string.IsNullOrEmpty(deltaTable.FileName) &&
                     deltaTable.Version >= 0 &&
                     deltaTable.CommitHistory.Any() &&
                     deltaTable.Protocol.MinReaderVersion > 0;
        
        _logger.LogInformation("Table {FileId} integrity validation: {IsValid}", fileId, isValid);
        
        return isValid;
    }

    private static bool IsTypeCompatible(string currentType, string newType)
    {
        // Simplified type compatibility matrix
        var compatibilityMap = new Dictionary<string, List<string>>
        {
            ["int"] = new() { "long", "double", "string" },
            ["long"] = new() { "double", "string" },
            ["float"] = new() { "double", "string" },
            ["double"] = new() { "string" },
            ["boolean"] = new() { "string" },
            ["date"] = new() { "timestamp", "string" },
            ["timestamp"] = new() { "string" }
        };

        return currentType == newType || 
               (compatibilityMap.TryGetValue(currentType.ToLower(), out var compatible) && 
                compatible.Contains(newType.ToLower()));
    }
}