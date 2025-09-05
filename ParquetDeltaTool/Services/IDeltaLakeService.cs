using ParquetDeltaTool.Models;

namespace ParquetDeltaTool.Services;

public interface IDeltaLakeService
{
    // Delta Table Operations
    Task<DeltaTableMetadata> CreateDeltaTableAsync(string tableName, TableSchema schema, Dictionary<string, string>? properties = null);
    Task<DeltaTableMetadata> GetDeltaTableMetadataAsync(Guid fileId);
    Task<List<DeltaCommitInfo>> GetCommitHistoryAsync(Guid fileId, int limit = 50);
    
    // Time Travel Operations
    Task<FileMetadata> ReadTableAtVersionAsync(Guid fileId, long version);
    Task<FileMetadata> ReadTableAtTimestampAsync(Guid fileId, DateTime timestamp);
    Task<List<long>> GetAvailableVersionsAsync(Guid fileId);
    
    // ACID Operations
    Task<long> MergeIntoTableAsync(Guid targetFileId, Guid sourceFileId, string mergeCondition, Dictionary<string, string> updateExpressions);
    Task<long> UpdateTableAsync(Guid fileId, string whereCondition, Dictionary<string, string> updateExpressions);
    Task<long> DeleteFromTableAsync(Guid fileId, string whereCondition);
    Task<long> InsertIntoTableAsync(Guid fileId, object[] rows);
    
    // Maintenance Operations
    Task<Dictionary<string, object>> OptimizeTableAsync(Guid fileId, List<string>? zOrderColumns = null);
    Task<Dictionary<string, object>> VacuumTableAsync(Guid fileId, TimeSpan retentionPeriod);
    Task<Dictionary<string, object>> CompactFilesAsync(Guid fileId, long targetFileSizeBytes = 1024 * 1024 * 128); // 128MB default
    
    // Schema Operations
    Task<TableSchema> GetTableSchemaAsync(Guid fileId, long? version = null);
    Task<bool> IsSchemaCompatibleAsync(TableSchema currentSchema, TableSchema newSchema);
    Task<DeltaTableMetadata> EvolveSchemaAsync(Guid fileId, TableSchema newSchema, bool allowDataLoss = false);
    
    // Transaction Log Operations
    Task<List<DeltaAction>> GetTransactionLogAsync(Guid fileId, long? startVersion = null, long? endVersion = null);
    Task<long> CreateCheckpointAsync(Guid fileId);
    Task<bool> ValidateTableIntegrityAsync(Guid fileId);
}