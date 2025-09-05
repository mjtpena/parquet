using ParquetDeltaTool.Models;

namespace ParquetDeltaTool.Services;

public interface IFileProcessor
{
    Task<FileMetadata> LoadFileAsync(Stream fileStream, FileFormat format);
    Task<DataPreview> GetPreviewAsync(Guid fileId, int rows = 100, int offset = 0);
    Task<Schema> GetSchemaAsync(Guid fileId);
    Task<Statistics> GetStatisticsAsync(Guid fileId, string? columnName = null);
    Task<byte[]> ExportAsync(Guid fileId, FileFormat targetFormat);
}

public interface IStorageService
{
    Task<Guid> StoreFileAsync(byte[] data, FileMetadata metadata);
    Task<byte[]> GetFileDataAsync(Guid fileId);
    Task<FileMetadata?> GetMetadataAsync(Guid fileId);
    Task<List<FileMetadata>> GetRecentFilesAsync(int count = 20);
    Task<bool> DeleteFileAsync(Guid fileId);
    Task<long> GetStorageUsageAsync();
    Task CleanupOldFilesAsync(TimeSpan retention);
}

public interface IQueryEngine
{
    Task<QueryResult> ExecuteQueryAsync(string sql, QueryOptions options);
    Task<ValidationResult> ValidateQueryAsync(string sql);
    Task<List<QueryResult>> GetQueryHistoryAsync(Guid? fileId = null, int count = 50);
}