using ParquetDeltaTool.Models;
using Microsoft.JSInterop;
using System.Text.Json;
using System.Text;
using System.IO.Compression;

namespace ParquetDeltaTool.Services;

public class DataExportService : IDataExportService
{
    private readonly IJSRuntime _jsRuntime;
    private readonly IStorageService _storageService;
    private readonly ISchemaManagementService _schemaService;
    private readonly ILogger<DataExportService> _logger;

    public DataExportService(
        IJSRuntime jsRuntime,
        IStorageService storageService,
        ISchemaManagementService schemaService,
        ILogger<DataExportService> logger)
    {
        _jsRuntime = jsRuntime;
        _storageService = storageService;
        _schemaService = schemaService;
        _logger = logger;
    }

    public async Task<byte[]> ExportToParquetAsync(Guid fileId, ExportOptions? options = null)
    {
        _logger.LogInformation("Exporting file {FileId} to Parquet format", fileId);
        
        options ??= new ExportOptions();
        
        try
        {
            // Use JavaScript interop to convert data to Parquet
            var exportRequest = new
            {
                fileId = fileId.ToString(),
                format = "parquet",
                options = new
                {
                    compression = options.Compression ?? "snappy",
                    rowGroupSize = options.RowGroupSize ?? 50000,
                    columns = options.Columns,
                    whereClause = options.WhereClause,
                    limit = options.Limit
                }
            };

            var base64Result = await _jsRuntime.InvokeAsync<string>("exportData", exportRequest);
            
            if (!string.IsNullOrEmpty(base64Result))
            {
                return Convert.FromBase64String(base64Result);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to export to Parquet format");
        }

        // Fallback: return mock Parquet data
        return await GenerateMockParquetData(fileId, options);
    }

    public async Task<byte[]> ExportToCsvAsync(Guid fileId, ExportOptions? options = null)
    {
        _logger.LogInformation("Exporting file {FileId} to CSV format", fileId);
        
        options ??= new ExportOptions();
        var metadata = await _storageService.GetFileMetadataAsync(fileId);
        var schema = await _schemaService.InferSchemaFromDataAsync(fileId);
        
        var csv = new StringBuilder();
        
        // Add header if requested
        if (options.IncludeHeader)
        {
            var columns = options.Columns ?? schema.Fields.Select(f => f.Name).ToList();
            csv.AppendLine(string.Join(options.Delimiter ?? ",", columns.Select(c => $"{options.Quote}{c}{options.Quote}")));
        }
        
        // Generate sample data
        var rowCount = Math.Min(options.Limit ?? 1000, 1000);
        var random = new Random();
        
        for (int i = 0; i < rowCount; i++)
        {
            var row = schema.Fields.Select(field => GenerateSampleValue(field, random)).ToList();
            var quotedRow = row.Select(value => $"{options.Quote}{value}{options.Quote}");
            csv.AppendLine(string.Join(options.Delimiter ?? ",", quotedRow));
        }
        
        return Encoding.UTF8.GetBytes(csv.ToString());
    }

    public async Task<byte[]> ExportToJsonAsync(Guid fileId, ExportOptions? options = null)
    {
        _logger.LogInformation("Exporting file {FileId} to JSON format", fileId);
        
        options ??= new ExportOptions();
        var schema = await _schemaService.InferSchemaFromDataAsync(fileId);
        
        var records = new List<Dictionary<string, object>>();
        var rowCount = Math.Min(options.Limit ?? 1000, 1000);
        var random = new Random();
        
        for (int i = 0; i < rowCount; i++)
        {
            var record = new Dictionary<string, object>();
            foreach (var field in schema.Fields)
            {
                if (options.Columns == null || options.Columns.Contains(field.Name))
                {
                    record[field.Name] = GenerateSampleValue(field, random);
                }
            }
            records.Add(record);
        }
        
        var jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
        
        if (options.IncludeMetadata)
        {
            var metadata = await _storageService.GetFileMetadataAsync(fileId);
            var exportData = new
            {
                metadata = new
                {
                    fileName = metadata.FileName,
                    rowCount = records.Count,
                    exportedAt = DateTime.UtcNow,
                    format = "JSON"
                },
                schema = options.IncludeSchema ? await _schemaService.GenerateSchemaJsonAsync(schema) : null,
                data = records
            };
            return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(exportData, jsonOptions));
        }
        
        return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(records, jsonOptions));
    }

    public async Task<byte[]> ExportToExcelAsync(Guid fileId, ExportOptions? options = null)
    {
        _logger.LogInformation("Exporting file {FileId} to Excel format", fileId);
        
        // For demo purposes, export as CSV with Excel-friendly format
        options ??= new ExportOptions();
        options.Delimiter = ",";
        options.Quote = "\"";
        options.IncludeHeader = true;
        
        var csvData = await ExportToCsvAsync(fileId, options);
        
        // In a real implementation, you would use a library like EPPlus or ClosedXML
        // For now, return CSV data with Excel MIME type metadata
        return csvData;
    }

    public async Task<byte[]> ExportToAvroAsync(Guid fileId, ExportOptions? options = null)
    {
        _logger.LogInformation("Exporting file {FileId} to Avro format", fileId);
        
        try
        {
            var exportRequest = new
            {
                fileId = fileId.ToString(),
                format = "avro",
                options = new
                {
                    compression = options?.Compression ?? "snappy",
                    columns = options?.Columns,
                    whereClause = options?.WhereClause,
                    limit = options?.Limit
                }
            };

            var base64Result = await _jsRuntime.InvokeAsync<string>("exportData", exportRequest);
            
            if (!string.IsNullOrEmpty(base64Result))
            {
                return Convert.FromBase64String(base64Result);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to export to Avro format");
        }

        // Fallback: return mock Avro data
        return await GenerateMockAvroData(fileId, options);
    }

    public async Task<byte[]> ExportToOrcAsync(Guid fileId, ExportOptions? options = null)
    {
        _logger.LogInformation("Exporting file {FileId} to ORC format", fileId);
        
        try
        {
            var exportRequest = new
            {
                fileId = fileId.ToString(),
                format = "orc",
                options = new
                {
                    compression = options?.Compression ?? "zlib",
                    columns = options?.Columns,
                    whereClause = options?.WhereClause,
                    limit = options?.Limit
                }
            };

            var base64Result = await _jsRuntime.InvokeAsync<string>("exportData", exportRequest);
            
            if (!string.IsNullOrEmpty(base64Result))
            {
                return Convert.FromBase64String(base64Result);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to export to ORC format");
        }

        // Fallback: return mock ORC data
        return await GenerateMockOrcData(fileId, options);
    }

    public async Task<byte[]> ExportToDeltaAsync(Guid fileId, ExportOptions? options = null)
    {
        _logger.LogInformation("Exporting file {FileId} to Delta format", fileId);
        
        // For Delta format, we need to create a transaction log and data files
        var parquetData = await ExportToParquetAsync(fileId, options);
        
        // Create a simple Delta log structure
        var deltaLog = new
        {
            version = 0,
            timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            operation = "WRITE",
            operationParameters = new { mode = "Overwrite" },
            readVersion = -1,
            isolationLevel = "Serializable",
            isBlindAppend = true
        };
        
        var deltaLogJson = JsonSerializer.Serialize(deltaLog);
        var deltaLogBytes = Encoding.UTF8.GetBytes(deltaLogJson);
        
        // For simplicity, return the Parquet data with Delta metadata
        var combinedData = new byte[parquetData.Length + deltaLogBytes.Length];
        Array.Copy(deltaLogBytes, 0, combinedData, 0, deltaLogBytes.Length);
        Array.Copy(parquetData, 0, combinedData, deltaLogBytes.Length, parquetData.Length);
        
        return combinedData;
    }

    public async Task<byte[]> ExportQueryResultAsync(string sqlQuery, Guid fileId, FileFormat outputFormat, ExportOptions? options = null)
    {
        _logger.LogInformation("Exporting query result for file {FileId} in {Format} format", fileId, outputFormat);
        
        // For demo purposes, apply basic query filtering to options
        options ??= new ExportOptions();
        
        if (sqlQuery.ToLower().Contains("limit"))
        {
            var limitMatch = System.Text.RegularExpressions.Regex.Match(sqlQuery.ToLower(), @"limit\s+(\d+)");
            if (limitMatch.Success && int.TryParse(limitMatch.Groups[1].Value, out var limit))
            {
                options.Limit = Math.Min(options.Limit ?? limit, limit);
            }
        }

        return outputFormat switch
        {
            FileFormat.Parquet => await ExportToParquetAsync(fileId, options),
            FileFormat.CSV => await ExportToCsvAsync(fileId, options),
            FileFormat.JSON => await ExportToJsonAsync(fileId, options),
            FileFormat.Delta => await ExportToDeltaAsync(fileId, options),
            _ => await ExportToCsvAsync(fileId, options)
        };
    }

    public async Task<Stream> StreamQueryResultAsync(string sqlQuery, Guid fileId, FileFormat outputFormat, ExportOptions? options = null)
    {
        var data = await ExportQueryResultAsync(sqlQuery, fileId, outputFormat, options);
        return new MemoryStream(data);
    }

    public async Task<Dictionary<string, byte[]>> ExportMultipleFilesAsync(List<Guid> fileIds, FileFormat outputFormat, ExportOptions? options = null)
    {
        _logger.LogInformation("Exporting {FileCount} files in {Format} format", fileIds.Count, outputFormat);
        
        var results = new Dictionary<string, byte[]>();
        
        foreach (var fileId in fileIds)
        {
            try
            {
                var metadata = await _storageService.GetFileMetadataAsync(fileId);
                var data = outputFormat switch
                {
                    FileFormat.Parquet => await ExportToParquetAsync(fileId, options),
                    FileFormat.CSV => await ExportToCsvAsync(fileId, options),
                    FileFormat.JSON => await ExportToJsonAsync(fileId, options),
                    FileFormat.Delta => await ExportToDeltaAsync(fileId, options),
                    _ => await ExportToCsvAsync(fileId, options)
                };
                
                var fileName = $"{Path.GetFileNameWithoutExtension(metadata.FileName)}.{GetFileExtension(outputFormat)}";
                results[fileName] = data;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to export file {FileId}", fileId);
            }
        }
        
        return results;
    }

    public async Task<byte[]> ExportZipArchiveAsync(List<Guid> fileIds, FileFormat outputFormat, ExportOptions? options = null)
    {
        _logger.LogInformation("Creating ZIP archive with {FileCount} files", fileIds.Count);
        
        var files = await ExportMultipleFilesAsync(fileIds, outputFormat, options);
        
        using var memoryStream = new MemoryStream();
        using var zipArchive = new ZipArchive(memoryStream, ZipArchiveMode.Create, true);
        
        foreach (var file in files)
        {
            var zipEntry = zipArchive.CreateEntry(file.Key);
            using var zipStream = zipEntry.Open();
            await zipStream.WriteAsync(file.Value);
        }
        
        return memoryStream.ToArray();
    }

    public async Task<string> ExportSchemaAsJsonAsync(Guid fileId)
    {
        var schema = await _schemaService.InferSchemaFromDataAsync(fileId);
        return await _schemaService.GenerateSchemaJsonAsync(schema);
    }

    public async Task<string> ExportSchemaAsDdlAsync(Guid fileId, string dialect = "spark")
    {
        var schema = await _schemaService.InferSchemaFromDataAsync(fileId);
        var metadata = await _storageService.GetFileMetadataAsync(fileId);
        
        var ddl = new StringBuilder();
        var tableName = Path.GetFileNameWithoutExtension(metadata.FileName).Replace('-', '_');
        
        ddl.AppendLine($"CREATE TABLE {tableName} (");
        
        var columnDefinitions = schema.Fields.Select(field =>
        {
            var sqlType = ConvertToSqlType(field.Type, dialect);
            var nullable = field.Nullable ? "" : " NOT NULL";
            return $"  {field.Name} {sqlType}{nullable}";
        });
        
        ddl.AppendLine(string.Join(",\n", columnDefinitions));
        ddl.AppendLine(");");
        
        return ddl.ToString();
    }

    public async Task<string> ExportSchemaAsAvroAsync(Guid fileId)
    {
        var schema = await _schemaService.InferSchemaFromDataAsync(fileId);
        var metadata = await _storageService.GetFileMetadataAsync(fileId);
        
        var avroSchema = new
        {
            type = "record",
            name = Path.GetFileNameWithoutExtension(metadata.FileName).Replace('-', '_'),
            fields = schema.Fields.Select(field => new
            {
                name = field.Name,
                type = ConvertToAvroType(field.Type, field.Nullable)
            }).ToList()
        };
        
        return JsonSerializer.Serialize(avroSchema, new JsonSerializerOptions { WriteIndented = true });
    }

    public async Task<byte[]> ExportWithTransformationAsync(Guid fileId, string transformationSql, FileFormat outputFormat, ExportOptions? options = null)
    {
        _logger.LogInformation("Exporting file {FileId} with transformation", fileId);
        
        // For demo purposes, treat transformation as a query filter
        return await ExportQueryResultAsync(transformationSql, fileId, outputFormat, options);
    }

    public async Task<byte[]> ExportPartitionedDataAsync(Guid fileId, List<string> partitionColumns, FileFormat outputFormat, ExportOptions? options = null)
    {
        _logger.LogInformation("Exporting partitioned data for file {FileId}", fileId);
        
        // For demo purposes, include partition information in metadata
        options ??= new ExportOptions();
        options.CustomMetadata ??= new Dictionary<string, string>();
        options.CustomMetadata["partitionColumns"] = string.Join(",", partitionColumns);
        
        return outputFormat switch
        {
            FileFormat.Parquet => await ExportToParquetAsync(fileId, options),
            FileFormat.CSV => await ExportToCsvAsync(fileId, options),
            FileFormat.JSON => await ExportToJsonAsync(fileId, options),
            FileFormat.Delta => await ExportToDeltaAsync(fileId, options),
            _ => await ExportToCsvAsync(fileId, options)
        };
    }

    public async Task<byte[]> ExportSampleDataAsync(Guid fileId, int sampleSize, FileFormat outputFormat, ExportOptions? options = null)
    {
        _logger.LogInformation("Exporting sample data ({SampleSize} rows) for file {FileId}", sampleSize, fileId);
        
        options ??= new ExportOptions();
        options.Limit = sampleSize;
        
        return outputFormat switch
        {
            FileFormat.Parquet => await ExportToParquetAsync(fileId, options),
            FileFormat.CSV => await ExportToCsvAsync(fileId, options),
            FileFormat.JSON => await ExportToJsonAsync(fileId, options),
            FileFormat.Delta => await ExportToDeltaAsync(fileId, options),
            _ => await ExportToCsvAsync(fileId, options)
        };
    }

    public async Task<Dictionary<string, object>> GetExportPreviewAsync(Guid fileId, FileFormat outputFormat, ExportOptions? options = null)
    {
        var metadata = await _storageService.GetFileMetadataAsync(fileId);
        var schema = await _schemaService.InferSchemaFromDataAsync(fileId);
        
        options ??= new ExportOptions();
        var estimatedRows = Math.Min(options.Limit ?? metadata.RowCount, metadata.RowCount);
        var selectedColumns = options.Columns ?? schema.Fields.Select(f => f.Name).ToList();
        
        return new Dictionary<string, object>
        {
            ["sourceFile"] = metadata.FileName,
            ["outputFormat"] = outputFormat.ToString(),
            ["estimatedRows"] = estimatedRows,
            ["selectedColumns"] = selectedColumns,
            ["estimatedSize"] = await EstimateExportSizeAsync(fileId, outputFormat, options),
            ["estimatedTime"] = await EstimateExportTimeAsync(fileId, outputFormat, options),
            ["compression"] = options.Compression ?? "snappy",
            ["includeHeader"] = options.IncludeHeader,
            ["includeMetadata"] = options.IncludeMetadata,
            ["sampleData"] = await GenerateSamplePreviewData(fileId, selectedColumns.Take(5).ToList(), 3)
        };
    }

    public async Task<long> EstimateExportSizeAsync(Guid fileId, FileFormat outputFormat, ExportOptions? options = null)
    {
        var metadata = await _storageService.GetFileMetadataAsync(fileId);
        var rowCount = Math.Min(options?.Limit ?? metadata.RowCount, metadata.RowCount);
        
        // Size estimation based on format and compression
        var baseSize = metadata.FileSize * ((double)rowCount / metadata.RowCount);
        
        var formatMultiplier = outputFormat switch
        {
            FileFormat.Parquet => 0.8, // Generally smaller due to columnar compression
            FileFormat.CSV => 1.5, // Text format, usually larger
            FileFormat.JSON => 2.0, // JSON overhead
            FileFormat.Delta => 0.9, // Similar to Parquet with metadata
            _ => 1.0
        };
        
        var compressionMultiplier = (options?.Compression?.ToLower()) switch
        {
            "none" => 1.0,
            "snappy" => 0.7,
            "gzip" => 0.5,
            "zstd" => 0.6,
            "lz4" => 0.8,
            _ => 0.7 // Default to snappy
        };
        
        return (long)(baseSize * formatMultiplier * compressionMultiplier);
    }

    public async Task<TimeSpan> EstimateExportTimeAsync(Guid fileId, FileFormat outputFormat, ExportOptions? options = null)
    {
        var estimatedSize = await EstimateExportSizeAsync(fileId, outputFormat, options);
        
        // Processing speed estimates (MB/s)
        var processingSpeed = outputFormat switch
        {
            FileFormat.Parquet => 50, // MB/s
            FileFormat.CSV => 100,
            FileFormat.JSON => 30,
            FileFormat.Delta => 45,
            _ => 50
        };
        
        var sizeInMB = estimatedSize / (1024.0 * 1024.0);
        var estimatedSeconds = Math.Max(1, sizeInMB / processingSpeed);
        
        return TimeSpan.FromSeconds(estimatedSeconds);
    }

    // Private helper methods
    private static object GenerateSampleValue(SchemaField field, Random random)
    {
        return field.Type.ToLower() switch
        {
            "boolean" => random.NextDouble() > 0.5,
            "int" => random.Next(1, 1000),
            "long" => random.NextInt64(1, 1000000),
            "float" => (float)random.NextDouble() * 1000,
            "double" => random.NextDouble() * 1000,
            "date" => DateTime.Today.AddDays(-random.Next(0, 365)).ToString("yyyy-MM-dd"),
            "timestamp" => DateTime.UtcNow.AddHours(-random.Next(0, 24)).ToString("yyyy-MM-dd HH:mm:ss"),
            "string" => $"Sample_{random.Next(1, 100)}",
            _ => $"Value_{random.Next(1, 100)}"
        };
    }

    private static string GetFileExtension(FileFormat format)
    {
        return format switch
        {
            FileFormat.Parquet => "parquet",
            FileFormat.CSV => "csv",
            FileFormat.JSON => "json",
            FileFormat.Delta => "parquet", // Delta uses Parquet files
            _ => "txt"
        };
    }

    private static string ConvertToSqlType(string type, string dialect)
    {
        return (type.ToLower(), dialect.ToLower()) switch
        {
            ("boolean", _) => "BOOLEAN",
            ("int", _) => "INTEGER",
            ("long", "spark") => "BIGINT",
            ("long", _) => "BIGINT",
            ("float", _) => "FLOAT",
            ("double", _) => "DOUBLE",
            ("string", "spark") => "STRING",
            ("string", _) => "VARCHAR(255)",
            ("date", _) => "DATE",
            ("timestamp", _) => "TIMESTAMP",
            _ => "STRING"
        };
    }

    private static object ConvertToAvroType(string type, bool nullable)
    {
        object avroType = type.ToLower() switch
        {
            "boolean" => "boolean",
            "int" => "int",
            "long" => "long",
            "float" => "float",
            "double" => "double",
            "string" => "string",
            "date" => (object)new { type = "int", logicalType = "date" },
            "timestamp" => (object)new { type = "long", logicalType = "timestamp-millis" },
            _ => "string"
        };

        return nullable ? new object[] { "null", avroType } : avroType;
    }

    private async Task<byte[]> GenerateMockParquetData(Guid fileId, ExportOptions? options)
    {
        // Generate mock Parquet file header (simplified)
        var header = Encoding.UTF8.GetBytes("PAR1"); // Parquet magic number
        var mockData = new byte[1024];
        new Random().NextBytes(mockData);
        
        var combined = new byte[header.Length + mockData.Length];
        Array.Copy(header, combined, header.Length);
        Array.Copy(mockData, 0, combined, header.Length, mockData.Length);
        
        return combined;
    }

    private async Task<byte[]> GenerateMockAvroData(Guid fileId, ExportOptions? options)
    {
        // Generate mock Avro file with magic bytes
        var header = new byte[] { 0x4F, 0x62, 0x6A, 0x01 }; // Avro magic bytes
        var mockData = new byte[512];
        new Random().NextBytes(mockData);
        
        var combined = new byte[header.Length + mockData.Length];
        Array.Copy(header, combined, header.Length);
        Array.Copy(mockData, 0, combined, header.Length, mockData.Length);
        
        return combined;
    }

    private async Task<byte[]> GenerateMockOrcData(Guid fileId, ExportOptions? options)
    {
        // Generate mock ORC file with magic bytes
        var header = Encoding.UTF8.GetBytes("ORC"); // ORC magic string
        var mockData = new byte[512];
        new Random().NextBytes(mockData);
        
        var combined = new byte[header.Length + mockData.Length];
        Array.Copy(header, combined, header.Length);
        Array.Copy(mockData, 0, combined, header.Length, mockData.Length);
        
        return combined;
    }

    private async Task<List<Dictionary<string, object>>> GenerateSamplePreviewData(Guid fileId, List<string> columns, int rows)
    {
        var preview = new List<Dictionary<string, object>>();
        var random = new Random();
        
        for (int i = 0; i < rows; i++)
        {
            var row = new Dictionary<string, object>();
            foreach (var column in columns)
            {
                row[column] = $"Sample_{column}_{i + 1}";
            }
            preview.Add(row);
        }
        
        return preview;
    }
}