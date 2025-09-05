using ParquetDeltaTool.Models;
using Microsoft.JSInterop;
using System.Text.Json;

namespace ParquetDeltaTool.Services;

public class FileProcessorService : IFileProcessor
{
    private readonly IJSRuntime _jsRuntime;
    private readonly IStorageService _storage;
    private readonly ILogger<FileProcessorService> _logger;

    public FileProcessorService(IJSRuntime jsRuntime, IStorageService storage, ILogger<FileProcessorService> logger)
    {
        _jsRuntime = jsRuntime;
        _storage = storage;
        _logger = logger;
    }

    public async Task<FileMetadata> LoadFileAsync(Stream fileStream, FileFormat format)
    {
        try
        {
            // Convert stream to byte array
            using var memoryStream = new MemoryStream();
            await fileStream.CopyToAsync(memoryStream);
            var bytes = memoryStream.ToArray();

            // Create basic metadata
            var metadata = new ParquetFileMetadata
            {
                FileId = Guid.NewGuid(),
                FileName = "uploaded-file",
                FileSize = bytes.Length,
                Format = format,
                CreatedAt = DateTime.UtcNow,
                ModifiedAt = DateTime.UtcNow
            };

            // For now, just store the file - we'll add Parquet parsing later
            if (format == FileFormat.Parquet)
            {
                // TODO: Parse Parquet metadata using WASM
                await ParseParquetMetadata(bytes, metadata);
            }
            else if (format == FileFormat.CSV)
            {
                await ParseCsvMetadata(bytes, metadata);
            }

            // Store file
            await _storage.StoreFileAsync(bytes, metadata);

            return metadata;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load file");
            throw;
        }
    }

    public async Task<DataPreview> GetPreviewAsync(Guid fileId, int rows = 100, int offset = 0)
    {
        try
        {
            var metadata = await _storage.GetMetadataAsync(fileId);
            if (metadata == null)
                throw new ArgumentException($"File with ID {fileId} not found");

            var fileData = await _storage.GetFileDataAsync(fileId);
            
            return metadata.Format switch
            {
                FileFormat.Parquet => await GetParquetPreview(fileData, rows, offset),
                FileFormat.CSV => await GetCsvPreview(fileData, rows, offset),
                FileFormat.JSON => await GetJsonPreview(fileData, rows, offset),
                _ => throw new NotSupportedException($"Format {metadata.Format} not supported")
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get preview for file {FileId}", fileId);
            throw;
        }
    }

    public async Task<Schema> GetSchemaAsync(Guid fileId)
    {
        var metadata = await _storage.GetMetadataAsync(fileId);
        if (metadata == null)
            throw new ArgumentException($"File with ID {fileId} not found");

        if (metadata is ParquetFileMetadata parquetMetadata)
        {
            return new Schema
            {
                Name = metadata.FileName,
                Fields = parquetMetadata.Columns.Select(c => new Field
                {
                    Name = c.Name,
                    DataType = c.DataType,
                    IsNullable = c.IsNullable,
                    Metadata = c.Metadata
                }).ToList()
            };
        }

        // For other formats, return basic schema
        return new Schema
        {
            Name = metadata.FileName,
            Fields = new List<Field>()
        };
    }

    public async Task<Statistics> GetStatisticsAsync(Guid fileId, string? columnName = null)
    {
        // TODO: Implement statistics computation
        await Task.Delay(1);
        return new Statistics();
    }

    public async Task<byte[]> ExportAsync(Guid fileId, FileFormat targetFormat)
    {
        var fileData = await _storage.GetFileDataAsync(fileId);
        // TODO: Implement format conversion
        return fileData;
    }

    private async Task ParseParquetMetadata(byte[] bytes, ParquetFileMetadata metadata)
    {
        // TODO: Use Parquet WASM to parse metadata
        // For now, just set some dummy data
        metadata.RowCount = 1000; // Placeholder
        metadata.RowGroups = 1;
        metadata.Version = "1.0";
        
        // Add some sample columns
        metadata.Columns.Add(new ColumnMetadata
        {
            Name = "id",
            DataType = "int64",
            IsNullable = false
        });
        metadata.Columns.Add(new ColumnMetadata
        {
            Name = "name",
            DataType = "string",
            IsNullable = true
        });
        
        await Task.Delay(1); // Placeholder for async work
    }

    private async Task ParseCsvMetadata(byte[] bytes, ParquetFileMetadata metadata)
    {
        // Basic CSV parsing to infer schema
        var content = System.Text.Encoding.UTF8.GetString(bytes);
        var lines = content.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        
        if (lines.Length > 0)
        {
            var headers = lines[0].Split(',');
            metadata.RowCount = lines.Length - 1; // Exclude header
            
            foreach (var header in headers)
            {
                metadata.Columns.Add(new ColumnMetadata
                {
                    Name = header.Trim(),
                    DataType = "string", // Default to string for CSV
                    IsNullable = true
                });
            }
        }
        
        await Task.Delay(1);
    }

    private async Task<DataPreview> GetParquetPreview(byte[] fileData, int rows, int offset)
    {
        // TODO: Use Parquet WASM to read actual data
        // For now, return sample data
        var preview = new DataPreview
        {
            TotalRows = 1000,
            ReturnedRows = Math.Min(rows, 1000),
            Schema = new Schema
            {
                Fields = new List<Field>
                {
                    new() { Name = "id", DataType = "int64" },
                    new() { Name = "name", DataType = "string" },
                    new() { Name = "value", DataType = "double" }
                }
            }
        };

        // Generate sample rows
        for (int i = offset; i < offset + rows && i < 1000; i++)
        {
            preview.Rows.Add(new Dictionary<string, object?>
            {
                ["id"] = i + 1,
                ["name"] = $"Record {i + 1}",
                ["value"] = Random.Shared.NextDouble() * 100
            });
        }

        await Task.Delay(1);
        return preview;
    }

    private async Task<DataPreview> GetCsvPreview(byte[] fileData, int rows, int offset)
    {
        var content = System.Text.Encoding.UTF8.GetString(fileData);
        var lines = content.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        
        var preview = new DataPreview
        {
            TotalRows = lines.Length - 1 // Exclude header
        };

        if (lines.Length > 0)
        {
            var headers = lines[0].Split(',');
            preview.Schema = new Schema
            {
                Fields = headers.Select(h => new Field { Name = h.Trim(), DataType = "string" }).ToList()
            };

            // Read data rows
            for (int i = 1 + offset; i < Math.Min(1 + offset + rows, lines.Length); i++)
            {
                var values = lines[i].Split(',');
                var row = new Dictionary<string, object?>();
                
                for (int j = 0; j < Math.Min(headers.Length, values.Length); j++)
                {
                    row[headers[j].Trim()] = values[j].Trim();
                }
                
                preview.Rows.Add(row);
            }
            
            preview.ReturnedRows = preview.Rows.Count;
        }

        await Task.Delay(1);
        return preview;
    }

    private async Task<DataPreview> GetJsonPreview(byte[] fileData, int rows, int offset)
    {
        var content = System.Text.Encoding.UTF8.GetString(fileData);
        
        // Try to parse as JSON Lines or JSON array
        var preview = new DataPreview();
        
        try
        {
            // Try JSON array first
            var jsonArray = JsonSerializer.Deserialize<JsonElement[]>(content);
            if (jsonArray != null)
            {
                preview.TotalRows = jsonArray.Length;
                
                // Infer schema from first element
                if (jsonArray.Length > 0)
                {
                    var firstElement = jsonArray[0];
                    preview.Schema = InferSchemaFromJsonElement(firstElement);
                }

                // Get data rows
                for (int i = offset; i < Math.Min(offset + rows, jsonArray.Length); i++)
                {
                    var row = JsonElementToDictionary(jsonArray[i]);
                    preview.Rows.Add(row);
                }
                
                preview.ReturnedRows = preview.Rows.Count;
            }
        }
        catch
        {
            // Try JSON Lines format
            var lines = content.Split('\n', StringSplitOptions.RemoveEmptyEntries);
            preview.TotalRows = lines.Length;
            
            if (lines.Length > 0)
            {
                try
                {
                    var firstLine = JsonSerializer.Deserialize<JsonElement>(lines[0]);
                    preview.Schema = InferSchemaFromJsonElement(firstLine);
                    
                    for (int i = offset; i < Math.Min(offset + rows, lines.Length); i++)
                    {
                        var element = JsonSerializer.Deserialize<JsonElement>(lines[i]);
                        var row = JsonElementToDictionary(element);
                        preview.Rows.Add(row);
                    }
                    
                    preview.ReturnedRows = preview.Rows.Count;
                }
                catch
                {
                    // Failed to parse JSON
                    preview.Schema = new Schema();
                }
            }
        }

        await Task.Delay(1);
        return preview;
    }

    private Schema InferSchemaFromJsonElement(JsonElement element)
    {
        var fields = new List<Field>();
        
        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in element.EnumerateObject())
            {
                var dataType = property.Value.ValueKind switch
                {
                    JsonValueKind.String => "string",
                    JsonValueKind.Number => "double",
                    JsonValueKind.True or JsonValueKind.False => "boolean",
                    JsonValueKind.Array => "array",
                    JsonValueKind.Object => "object",
                    _ => "string"
                };
                
                fields.Add(new Field
                {
                    Name = property.Name,
                    DataType = dataType,
                    IsNullable = property.Value.ValueKind == JsonValueKind.Null
                });
            }
        }
        
        return new Schema { Fields = fields };
    }

    private Dictionary<string, object?> JsonElementToDictionary(JsonElement element)
    {
        var dict = new Dictionary<string, object?>();
        
        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in element.EnumerateObject())
            {
                dict[property.Name] = property.Value.ValueKind switch
                {
                    JsonValueKind.String => property.Value.GetString(),
                    JsonValueKind.Number => property.Value.GetDouble(),
                    JsonValueKind.True => true,
                    JsonValueKind.False => false,
                    JsonValueKind.Null => null,
                    _ => property.Value.ToString()
                };
            }
        }
        
        return dict;
    }
}