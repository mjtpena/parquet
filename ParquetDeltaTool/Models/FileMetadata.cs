namespace ParquetDeltaTool.Models;

public enum FileFormat
{
    Parquet,
    Delta,
    CSV,
    JSON
}

public enum CompressionType
{
    None,
    Snappy,
    Gzip,
    Brotli,
    LZ4,
    Lz4,
    ZSTD,
    Zstd
}

public class FileMetadata
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public Guid FileId { get; set; } = Guid.NewGuid();
    public string FileName { get; set; } = string.Empty;
    public long FileSize { get; set; }
    public long FileSizeBytes { get; set; }
    public long RowCount { get; set; }
    public FileFormat Format { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime ModifiedAt { get; set; } = DateTime.UtcNow;
    public string CreatedBy { get; set; } = string.Empty;
    public Dictionary<string, string> Properties { get; set; } = new();
    public CompressionType Compression { get; set; } = CompressionType.None;
    public TableSchema? Schema { get; set; }
}

public class ParquetFileMetadata : FileMetadata
{
    public int RowGroups { get; set; }
    public new long RowCount { get; set; }
    public List<RowGroupMetadata> RowGroupMetadata { get; set; } = new();
    public List<ColumnMetadata> Columns { get; set; } = new();
    public string Version { get; set; } = string.Empty;
    public Dictionary<string, byte[]> KeyValueMetadata { get; set; } = new();
}

public class RowGroupMetadata
{
    public int Index { get; set; }
    public long RowCount { get; set; }
    public long TotalByteSize { get; set; }
    public List<ColumnChunkMetadata> ColumnChunks { get; set; } = new();
    public long FileOffset { get; set; }
    public long TotalCompressedSize { get; set; }
    public int Ordinal { get; set; }
}

public class ColumnChunkMetadata
{
    public string ColumnPath { get; set; } = string.Empty;
    public long FileOffset { get; set; }
    public long CompressedSize { get; set; }
    public long UncompressedSize { get; set; }
    public long NumValues { get; set; }
    public Statistics? Statistics { get; set; }
    public string Encoding { get; set; } = string.Empty;
    public CompressionType Compression { get; set; }
}

public class Statistics
{
    public object? Min { get; set; }
    public object? Max { get; set; }
    public long NullCount { get; set; }
    public long DistinctCount { get; set; }
    public double? Mean { get; set; }
    public double? StandardDeviation { get; set; }
    public Dictionary<string, long> ValueCounts { get; set; } = new();
}

public class ColumnMetadata
{
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsNullable { get; set; }
    public Dictionary<string, string> Metadata { get; set; } = new();
    public Statistics? Statistics { get; set; }
}

public class FileStatistics
{
    public Guid FileId { get; set; }
    public long RowCount { get; set; }
    public long FileSizeBytes { get; set; }
    public int ColumnCount { get; set; }
    public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
    public Dictionary<string, ColumnStatistics> ColumnStats { get; set; } = new();
    public List<PerformanceInsight> PerformanceInsights { get; set; } = new();
    public List<DataQualityIssue> DataQualityIssues { get; set; } = new();
}

public class ColumnStatistics
{
    public string ColumnName { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public long NullCount { get; set; }
    public double NullPercentage { get; set; }
    public long DistinctCount { get; set; }
    public long UniqueCount { get; set; }
    public string? MinValue { get; set; }
    public string? MaxValue { get; set; }
    public double? MeanValue { get; set; }
    public double? StandardDeviation { get; set; }
    public List<string> TopValues { get; set; } = new();
    public Dictionary<string, int> ValueDistribution { get; set; } = new();
}

public class PerformanceInsight
{
    public string Type { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string Severity { get; set; } = string.Empty;
    public Dictionary<string, object> Metrics { get; set; } = new();
}

public class DataQualityIssue
{
    public string Type { get; set; } = string.Empty;
    public string ColumnName { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string Severity { get; set; } = string.Empty;
    public long AffectedRows { get; set; }
    public string Recommendation { get; set; } = string.Empty;
}

// Delta Lake Models
public class DeltaTableMetadata : FileMetadata
{
    public long Version { get; set; } = 0;
    public string MinReaderVersion { get; set; } = "1";
    public string MinWriterVersion { get; set; } = "2";
    public List<DeltaAction> Actions { get; set; } = new();
    public DeltaProtocol Protocol { get; set; } = new();
    public Dictionary<string, string> Configuration { get; set; } = new();
    public List<DeltaCommitInfo> CommitHistory { get; set; } = new();
    public string SchemaString { get; set; } = string.Empty;
    public List<string> PartitionColumns { get; set; } = new();
}

public class DeltaProtocol
{
    public int MinReaderVersion { get; set; } = 1;
    public int MinWriterVersion { get; set; } = 2;
    public List<string> ReaderFeatures { get; set; } = new();
    public List<string> WriterFeatures { get; set; } = new();
}

public class DeltaAction
{
    public string Action { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public Dictionary<string, object> Data { get; set; } = new();
}

public class DeltaCommitInfo
{
    public long Version { get; set; }
    public DateTime Timestamp { get; set; }
    public string UserId { get; set; } = string.Empty;
    public string UserName { get; set; } = string.Empty;
    public string Operation { get; set; } = string.Empty;
    public Dictionary<string, object> OperationParameters { get; set; } = new();
    public Dictionary<string, object> OperationMetrics { get; set; } = new();
    public string ClientVersion { get; set; } = string.Empty;
    public long ReadVersion { get; set; } = -1;
    public string IsolationLevel { get; set; } = "Serializable";
    public bool IsBlindAppend { get; set; } = false;
}

public class DeltaFileAction
{
    public string Path { get; set; } = string.Empty;
    public Dictionary<string, string> PartitionValues { get; set; } = new();
    public long Size { get; set; }
    public long ModificationTime { get; set; }
    public bool DataChange { get; set; } = true;
    public Dictionary<string, object> Stats { get; set; } = new();
    public Dictionary<string, string> Tags { get; set; } = new();
}

public class DeltaTimeTravel
{
    public long? Version { get; set; }
    public DateTime? Timestamp { get; set; }
    public string? TimestampString { get; set; }
}

public class SchemaField
{
    public string Name { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool Nullable { get; set; } = true;
    public bool IsNullable { get; set; } = true;
    public Dictionary<string, object> Metadata { get; set; } = new();
    public List<SchemaField> Fields { get; set; } = new(); // For nested structures
}

public class TableSchema
{
    public string Name { get; set; } = string.Empty;
    public string Type { get; set; } = "struct";
    public List<SchemaField> Fields { get; set; } = new();
}

public class QueryExecutionPlan
{
    public string QueryId { get; set; } = Guid.NewGuid().ToString();
    public string SqlQuery { get; set; } = string.Empty;
    public DateTime ExecutedAt { get; set; } = DateTime.UtcNow;
    public long ExecutionTimeMs { get; set; }
    public long RowsRead { get; set; }
    public long BytesScanned { get; set; }
    public List<ExecutionNode> Nodes { get; set; } = new();
    public Dictionary<string, object> Statistics { get; set; } = new();
    public List<string> Optimizations { get; set; } = new();
}

public class ExecutionNode
{
    public string NodeType { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public double Cost { get; set; }
    public long Rows { get; set; }
    public Dictionary<string, object> Properties { get; set; } = new();
    public List<ExecutionNode> Children { get; set; } = new();
}