namespace ParquetDeltaTool.Models;

public class Schema
{
    public string Name { get; set; } = string.Empty;
    public List<Field> Fields { get; set; } = new();
    public Dictionary<string, string> Metadata { get; set; } = new();
}

public class Field
{
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsNullable { get; set; }
    public object? DefaultValue { get; set; }
    public Dictionary<string, string> Metadata { get; set; } = new();
    public List<Field> Children { get; set; } = new(); // For nested types
}

public class DataPreview
{
    public List<Dictionary<string, object?>> Rows { get; set; } = new();
    public Schema? Schema { get; set; }
    public long TotalRows { get; set; }
    public int ReturnedRows { get; set; }
    public Dictionary<string, Statistics> Statistics { get; set; } = new();
}

public class QueryResult
{
    public Guid QueryId { get; set; } = Guid.NewGuid();
    public List<Column> Columns { get; set; } = new();
    public List<Dictionary<string, object?>> Rows { get; set; } = new();
    public long TotalRows { get; set; }
    public long ReturnedRows { get; set; }
    public TimeSpan ExecutionTime { get; set; }
    public long BytesScanned { get; set; }
    public string QueryText { get; set; } = string.Empty;
    public DateTime ExecutedAt { get; set; } = DateTime.UtcNow;
}

public class Column
{
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsNullable { get; set; }
    public bool IsVisible { get; set; } = true;
    public int Width { get; set; } = 150;
}

public class QueryOptions
{
    public Guid? FileId { get; set; }
    public int? MaxRows { get; set; } = 1000;
    public int? TimeoutSeconds { get; set; } = 30;
}

public class ProcessingOptions
{
    public FileFormat Format { get; set; }
    public bool InferSchema { get; set; } = true;
    public bool ComputeStatistics { get; set; } = true;
    public bool ValidateData { get; set; } = false;
    public int? PreviewRows { get; set; } = 100;
}

public class ValidationResult
{
    public bool IsValid { get; set; }
    public List<string> Errors { get; set; } = new();
    public List<string> Warnings { get; set; } = new();
}

public class FileUploadResponse
{
    public Guid FileId { get; set; }
    public FileMetadata? Metadata { get; set; }
    public TimeSpan ProcessingTime { get; set; }
    public bool Success { get; set; }
    public string? Error { get; set; }
}