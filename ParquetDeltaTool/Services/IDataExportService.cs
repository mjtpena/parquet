using ParquetDeltaTool.Models;

namespace ParquetDeltaTool.Services;

public interface IDataExportService
{
    // Export Operations
    Task<byte[]> ExportToParquetAsync(Guid fileId, ExportOptions? options = null);
    Task<byte[]> ExportToCsvAsync(Guid fileId, ExportOptions? options = null);
    Task<byte[]> ExportToJsonAsync(Guid fileId, ExportOptions? options = null);
    Task<byte[]> ExportToExcelAsync(Guid fileId, ExportOptions? options = null);
    Task<byte[]> ExportToAvroAsync(Guid fileId, ExportOptions? options = null);
    Task<byte[]> ExportToOrcAsync(Guid fileId, ExportOptions? options = null);
    Task<byte[]> ExportToDeltaAsync(Guid fileId, ExportOptions? options = null);
    
    // Query-based Export
    Task<byte[]> ExportQueryResultAsync(string sqlQuery, Guid fileId, FileFormat outputFormat, ExportOptions? options = null);
    Task<Stream> StreamQueryResultAsync(string sqlQuery, Guid fileId, FileFormat outputFormat, ExportOptions? options = null);
    
    // Batch Export Operations
    Task<Dictionary<string, byte[]>> ExportMultipleFilesAsync(List<Guid> fileIds, FileFormat outputFormat, ExportOptions? options = null);
    Task<byte[]> ExportZipArchiveAsync(List<Guid> fileIds, FileFormat outputFormat, ExportOptions? options = null);
    
    // Schema Export
    Task<string> ExportSchemaAsJsonAsync(Guid fileId);
    Task<string> ExportSchemaAsDdlAsync(Guid fileId, string dialect = "spark");
    Task<string> ExportSchemaAsAvroAsync(Guid fileId);
    
    // Advanced Export Features
    Task<byte[]> ExportWithTransformationAsync(Guid fileId, string transformationSql, FileFormat outputFormat, ExportOptions? options = null);
    Task<byte[]> ExportPartitionedDataAsync(Guid fileId, List<string> partitionColumns, FileFormat outputFormat, ExportOptions? options = null);
    Task<byte[]> ExportSampleDataAsync(Guid fileId, int sampleSize, FileFormat outputFormat, ExportOptions? options = null);
    
    // Export Metadata and Statistics
    Task<Dictionary<string, object>> GetExportPreviewAsync(Guid fileId, FileFormat outputFormat, ExportOptions? options = null);
    Task<long> EstimateExportSizeAsync(Guid fileId, FileFormat outputFormat, ExportOptions? options = null);
    Task<TimeSpan> EstimateExportTimeAsync(Guid fileId, FileFormat outputFormat, ExportOptions? options = null);
}

public class ExportOptions
{
    // Pagination and Limiting
    public int? Limit { get; set; }
    public int? Offset { get; set; }
    public List<string>? Columns { get; set; }
    public string? WhereClause { get; set; }
    
    // Format-specific Options
    public string? Compression { get; set; } = "snappy";
    public string? DateFormat { get; set; } = "yyyy-MM-dd";
    public string? TimestampFormat { get; set; } = "yyyy-MM-dd HH:mm:ss";
    public string? Delimiter { get; set; } = ",";
    public string? Quote { get; set; } = "\"";
    public string? Escape { get; set; } = "\\";
    public bool IncludeHeader { get; set; } = true;
    
    // Quality and Performance
    public int? MaxFileSize { get; set; } // Max file size in MB
    public int? RowGroupSize { get; set; } // For Parquet
    public int? BatchSize { get; set; } = 10000;
    public bool ValidateData { get; set; } = true;
    
    // Metadata Options
    public bool IncludeMetadata { get; set; } = false;
    public bool IncludeSchema { get; set; } = false;
    public Dictionary<string, string>? CustomMetadata { get; set; }
}