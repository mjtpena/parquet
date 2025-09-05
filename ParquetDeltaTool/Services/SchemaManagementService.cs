using ParquetDeltaTool.Models;
using Microsoft.JSInterop;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace ParquetDeltaTool.Services;

public class SchemaManagementService : ISchemaManagementService
{
    private readonly IJSRuntime _jsRuntime;
    private readonly IStorageService _storageService;
    private readonly IDeltaLakeService _deltaLakeService;
    private readonly ILogger<SchemaManagementService> _logger;

    public SchemaManagementService(
        IJSRuntime jsRuntime, 
        IStorageService storageService,
        IDeltaLakeService deltaLakeService,
        ILogger<SchemaManagementService> logger)
    {
        _jsRuntime = jsRuntime;
        _storageService = storageService;
        _deltaLakeService = deltaLakeService;
        _logger = logger;
    }

    public async Task<TableSchema> InferSchemaFromDataAsync(Guid fileId)
    {
        _logger.LogInformation("Inferring schema for file {FileId}", fileId);
        
        var metadata = await _storageService.GetFileMetadataAsync(fileId);
        
        if (metadata.Format == FileFormat.Parquet)
        {
            // Use JavaScript interop to get schema from Parquet file
            try
            {
                var schemaJson = await _jsRuntime.InvokeAsync<string>("getParquetSchema", fileId.ToString());
                if (!string.IsNullOrEmpty(schemaJson))
                {
                    var schemaData = JsonSerializer.Deserialize<Dictionary<string, object>>(schemaJson);
                    return ConvertArrowSchemaToTableSchema(schemaData);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Failed to extract schema from Parquet file: {Error}", ex.Message);
            }
        }

        // Fallback to sample-based schema inference
        return await InferSchemaFromSampleDataAsync(fileId);
    }

    public async Task<TableSchema> ExtractSchemaFromParquetAsync(byte[] parquetData)
    {
        _logger.LogInformation("Extracting schema from Parquet data ({Size} bytes)", parquetData.Length);
        
        try
        {
            // Use JavaScript interop to extract schema
            var base64Data = Convert.ToBase64String(parquetData);
            var schemaJson = await _jsRuntime.InvokeAsync<string>("extractParquetSchema", base64Data);
            
            if (!string.IsNullOrEmpty(schemaJson))
            {
                var schemaData = JsonSerializer.Deserialize<Dictionary<string, object>>(schemaJson);
                return ConvertArrowSchemaToTableSchema(schemaData);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Failed to extract schema from Parquet data: {Error}", ex.Message);
        }

        // Return default schema if extraction fails
        return new TableSchema
        {
            Fields = new List<SchemaField>
            {
                new() { Name = "unknown_column", Type = "string", Nullable = true }
            }
        };
    }

    public async Task<List<SchemaField>> GetColumnsAsync(Guid fileId)
    {
        var schema = await InferSchemaFromDataAsync(fileId);
        return schema.Fields;
    }

    public async Task<Dictionary<string, ColumnStatistics>> GetColumnStatisticsAsync(Guid fileId)
    {
        _logger.LogInformation("Calculating column statistics for file {FileId}", fileId);
        
        var schema = await InferSchemaFromDataAsync(fileId);
        var statistics = new Dictionary<string, ColumnStatistics>();

        foreach (var field in schema.Fields)
        {
            statistics[field.Name] = await GenerateColumnStatisticsAsync(fileId, field);
        }

        return statistics;
    }

    public async Task<bool> ValidateSchemaAsync(TableSchema schema)
    {
        var errors = await GetSchemaValidationErrorsAsync(schema);
        return errors.Count == 0;
    }

    public async Task<List<string>> GetSchemaValidationErrorsAsync(TableSchema schema)
    {
        var errors = new List<string>();

        if (schema.Fields == null || schema.Fields.Count == 0)
        {
            errors.Add("Schema must contain at least one field");
            return errors;
        }

        var fieldNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        
        foreach (var field in schema.Fields)
        {
            // Check for duplicate names
            if (!fieldNames.Add(field.Name))
            {
                errors.Add($"Duplicate field name: {field.Name}");
            }

            // Validate field name
            if (string.IsNullOrWhiteSpace(field.Name))
            {
                errors.Add("Field name cannot be empty");
            }
            else if (!IsValidFieldName(field.Name))
            {
                errors.Add($"Invalid field name: {field.Name}. Field names must start with letter/underscore and contain only alphanumeric characters and underscores");
            }

            // Validate field type
            if (string.IsNullOrWhiteSpace(field.Type))
            {
                errors.Add($"Field {field.Name} must have a type specified");
            }
            else if (!IsValidDataType(field.Type))
            {
                errors.Add($"Invalid data type for field {field.Name}: {field.Type}");
            }

            // Validate nested fields
            if (field.Fields?.Any() == true)
            {
                var nestedSchema = new TableSchema { Fields = field.Fields };
                var nestedErrors = await GetSchemaValidationErrorsAsync(nestedSchema);
                errors.AddRange(nestedErrors.Select(e => $"{field.Name}.{e}"));
            }
        }

        return errors;
    }

    public async Task<bool> IsBackwardCompatibleAsync(TableSchema oldSchema, TableSchema newSchema)
    {
        var issues = await GetCompatibilityIssuesAsync(oldSchema, newSchema);
        return issues.Count == 0;
    }

    public async Task<List<string>> GetCompatibilityIssuesAsync(TableSchema oldSchema, TableSchema newSchema)
    {
        var issues = new List<string>();
        var oldFields = oldSchema.Fields.ToDictionary(f => f.Name, f => f, StringComparer.OrdinalIgnoreCase);
        var newFields = newSchema.Fields.ToDictionary(f => f.Name, f => f, StringComparer.OrdinalIgnoreCase);

        // Check for removed fields
        foreach (var oldField in oldFields.Values)
        {
            if (!newFields.ContainsKey(oldField.Name))
            {
                issues.Add($"Field '{oldField.Name}' was removed");
            }
        }

        // Check for type changes and nullability changes
        foreach (var newField in newFields.Values)
        {
            if (oldFields.TryGetValue(newField.Name, out var oldField))
            {
                // Check type compatibility
                if (!AreTypesCompatible(oldField.Type, newField.Type))
                {
                    issues.Add($"Field '{newField.Name}' type changed from {oldField.Type} to {newField.Type} (incompatible)");
                }

                // Check nullability - making a nullable field non-nullable is breaking
                if (oldField.Nullable && !newField.Nullable)
                {
                    issues.Add($"Field '{newField.Name}' changed from nullable to non-nullable (breaking change)");
                }
            }
        }

        return issues;
    }

    public async Task<TableSchema> MergeSchemaAsync(TableSchema schema1, TableSchema schema2)
    {
        _logger.LogInformation("Merging two schemas");
        
        var mergedFields = new Dictionary<string, SchemaField>(StringComparer.OrdinalIgnoreCase);

        // Add fields from first schema
        foreach (var field in schema1.Fields)
        {
            mergedFields[field.Name] = new SchemaField
            {
                Name = field.Name,
                Type = field.Type,
                Nullable = field.Nullable,
                Metadata = new Dictionary<string, object>(field.Metadata),
                Fields = field.Fields?.Select(f => new SchemaField 
                { 
                    Name = f.Name, 
                    Type = f.Type, 
                    Nullable = f.Nullable,
                    Metadata = new Dictionary<string, object>(f.Metadata)
                }).ToList() ?? new List<SchemaField>()
            };
        }

        // Merge fields from second schema
        foreach (var field in schema2.Fields)
        {
            if (mergedFields.TryGetValue(field.Name, out var existingField))
            {
                // Merge existing field - make nullable if either is nullable
                existingField.Nullable = existingField.Nullable || field.Nullable;
                
                // Try to merge types (promote to more general type if different)
                existingField.Type = GetPromotedType(existingField.Type, field.Type);
                
                // Merge metadata
                foreach (var kvp in field.Metadata)
                {
                    existingField.Metadata[kvp.Key] = kvp.Value;
                }
            }
            else
            {
                // Add new field (make it nullable since it doesn't exist in first schema)
                mergedFields[field.Name] = new SchemaField
                {
                    Name = field.Name,
                    Type = field.Type,
                    Nullable = true, // New fields are nullable by default
                    Metadata = new Dictionary<string, object>(field.Metadata),
                    Fields = field.Fields?.ToList() ?? new List<SchemaField>()
                };
            }
        }

        return new TableSchema
        {
            Fields = mergedFields.Values.OrderBy(f => f.Name).ToList()
        };
    }

    public async Task<TableSchema> AddColumnAsync(TableSchema schema, SchemaField newColumn)
    {
        _logger.LogInformation("Adding column {ColumnName} to schema", newColumn.Name);
        
        var newSchema = new TableSchema
        {
            Type = schema.Type,
            Fields = new List<SchemaField>(schema.Fields)
        };

        // Check if column already exists
        if (newSchema.Fields.Any(f => f.Name.Equals(newColumn.Name, StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException($"Column '{newColumn.Name}' already exists in schema");
        }

        newSchema.Fields.Add(newColumn);
        return newSchema;
    }

    public async Task<TableSchema> DropColumnAsync(TableSchema schema, string columnName)
    {
        _logger.LogInformation("Dropping column {ColumnName} from schema", columnName);
        
        var newSchema = new TableSchema
        {
            Type = schema.Type,
            Fields = schema.Fields.Where(f => !f.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase)).ToList()
        };

        if (newSchema.Fields.Count == schema.Fields.Count)
        {
            throw new InvalidOperationException($"Column '{columnName}' not found in schema");
        }

        return newSchema;
    }

    public async Task<TableSchema> RenameColumnAsync(TableSchema schema, string oldName, string newName)
    {
        _logger.LogInformation("Renaming column {OldName} to {NewName}", oldName, newName);
        
        var newSchema = new TableSchema
        {
            Type = schema.Type,
            Fields = new List<SchemaField>()
        };

        bool found = false;
        foreach (var field in schema.Fields)
        {
            if (field.Name.Equals(oldName, StringComparison.OrdinalIgnoreCase))
            {
                newSchema.Fields.Add(new SchemaField
                {
                    Name = newName,
                    Type = field.Type,
                    Nullable = field.Nullable,
                    Metadata = new Dictionary<string, object>(field.Metadata),
                    Fields = field.Fields?.ToList() ?? new List<SchemaField>()
                });
                found = true;
            }
            else
            {
                newSchema.Fields.Add(field);
            }
        }

        if (!found)
        {
            throw new InvalidOperationException($"Column '{oldName}' not found in schema");
        }

        return newSchema;
    }

    public async Task<TableSchema> ChangeColumnTypeAsync(TableSchema schema, string columnName, string newType)
    {
        _logger.LogInformation("Changing column {ColumnName} type to {NewType}", columnName, newType);
        
        if (!IsValidDataType(newType))
        {
            throw new ArgumentException($"Invalid data type: {newType}");
        }

        var newSchema = new TableSchema
        {
            Type = schema.Type,
            Fields = new List<SchemaField>()
        };

        bool found = false;
        foreach (var field in schema.Fields)
        {
            if (field.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase))
            {
                newSchema.Fields.Add(new SchemaField
                {
                    Name = field.Name,
                    Type = newType,
                    Nullable = field.Nullable,
                    Metadata = new Dictionary<string, object>(field.Metadata),
                    Fields = field.Fields?.ToList() ?? new List<SchemaField>()
                });
                found = true;
            }
            else
            {
                newSchema.Fields.Add(field);
            }
        }

        if (!found)
        {
            throw new InvalidOperationException($"Column '{columnName}' not found in schema");
        }

        return newSchema;
    }

    public async Task<string> GenerateSchemaJsonAsync(TableSchema schema)
    {
        var options = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
        
        return JsonSerializer.Serialize(schema, options);
    }

    public async Task<string> GenerateSchemaDocumentationAsync(TableSchema schema)
    {
        var doc = new System.Text.StringBuilder();
        
        doc.AppendLine("# Table Schema Documentation");
        doc.AppendLine();
        doc.AppendLine($"**Schema Type:** {schema.Type}");
        doc.AppendLine($"**Total Fields:** {schema.Fields.Count}");
        doc.AppendLine();
        doc.AppendLine("## Fields");
        doc.AppendLine();
        
        foreach (var field in schema.Fields.OrderBy(f => f.Name))
        {
            doc.AppendLine($"### {field.Name}");
            doc.AppendLine($"- **Type:** {field.Type}");
            doc.AppendLine($"- **Nullable:** {(field.Nullable ? "Yes" : "No")}");
            
            if (field.Metadata.Any())
            {
                doc.AppendLine("- **Metadata:**");
                foreach (var meta in field.Metadata)
                {
                    doc.AppendLine($"  - {meta.Key}: {meta.Value}");
                }
            }
            
            if (field.Fields?.Any() == true)
            {
                doc.AppendLine("- **Nested Fields:**");
                foreach (var nestedField in field.Fields)
                {
                    doc.AppendLine($"  - {nestedField.Name}: {nestedField.Type} ({(nestedField.Nullable ? "nullable" : "required")})");
                }
            }
            
            doc.AppendLine();
        }
        
        return doc.ToString();
    }

    public async Task<Dictionary<string, object>> GetSchemaVisualizationDataAsync(TableSchema schema)
    {
        var visualizationData = new Dictionary<string, object>
        {
            ["nodes"] = schema.Fields.Select((field, index) => new
            {
                id = field.Name,
                label = field.Name,
                type = field.Type,
                nullable = field.Nullable,
                group = GetTypeGroup(field.Type),
                level = 0,
                index = index
            }).ToList(),
            
            ["edges"] = new List<object>(), // For nested relationships
            
            ["statistics"] = new Dictionary<string, object>
            {
                ["totalFields"] = schema.Fields.Count,
                ["nullableFields"] = schema.Fields.Count(f => f.Nullable),
                ["requiredFields"] = schema.Fields.Count(f => !f.Nullable),
                ["typeDistribution"] = schema.Fields
                    .GroupBy(f => f.Type)
                    .ToDictionary(g => g.Key, g => g.Count()),
                ["complexFields"] = schema.Fields.Count(f => f.Fields?.Any() == true)
            }
        };

        return visualizationData;
    }

    public async Task<List<TableSchema>> GetSchemaHistoryAsync(Guid fileId)
    {
        _logger.LogInformation("Getting schema history for file {FileId}", fileId);
        
        try
        {
            var deltaTable = await _deltaLakeService.GetDeltaTableMetadataAsync(fileId);
            var schemas = new List<TableSchema>();
            
            foreach (var commit in deltaTable.CommitHistory.OrderBy(c => c.Version))
            {
                try
                {
                    var schema = await _deltaLakeService.GetTableSchemaAsync(fileId, commit.Version);
                    schemas.Add(schema);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("Failed to get schema at version {Version}: {Error}", commit.Version, ex.Message);
                }
            }
            
            return schemas;
        }
        catch (Exception)
        {
            // Fallback to current schema only
            var currentSchema = await InferSchemaFromDataAsync(fileId);
            return new List<TableSchema> { currentSchema };
        }
    }

    public async Task<TableSchema> GetSchemaAtVersionAsync(Guid fileId, long version)
    {
        return await _deltaLakeService.GetTableSchemaAsync(fileId, version);
    }

    public async Task<Dictionary<string, object>> CompareSchemaVersionsAsync(TableSchema schema1, TableSchema schema2)
    {
        var comparison = new Dictionary<string, object>();
        
        var fields1 = schema1.Fields.ToDictionary(f => f.Name, f => f, StringComparer.OrdinalIgnoreCase);
        var fields2 = schema2.Fields.ToDictionary(f => f.Name, f => f, StringComparer.OrdinalIgnoreCase);
        
        var addedFields = fields2.Keys.Except(fields1.Keys, StringComparer.OrdinalIgnoreCase).ToList();
        var removedFields = fields1.Keys.Except(fields2.Keys, StringComparer.OrdinalIgnoreCase).ToList();
        var commonFields = fields1.Keys.Intersect(fields2.Keys, StringComparer.OrdinalIgnoreCase).ToList();
        
        var modifiedFields = new List<object>();
        foreach (var fieldName in commonFields)
        {
            var field1 = fields1[fieldName];
            var field2 = fields2[fieldName];
            
            if (field1.Type != field2.Type || field1.Nullable != field2.Nullable)
            {
                modifiedFields.Add(new
                {
                    name = fieldName,
                    oldType = field1.Type,
                    newType = field2.Type,
                    oldNullable = field1.Nullable,
                    newNullable = field2.Nullable
                });
            }
        }
        
        comparison["addedFields"] = addedFields;
        comparison["removedFields"] = removedFields;
        comparison["modifiedFields"] = modifiedFields;
        comparison["unchangedFields"] = commonFields.Except(modifiedFields.Select(m => ((dynamic)m).name)).ToList();
        comparison["isCompatible"] = await IsBackwardCompatibleAsync(schema1, schema2);
        comparison["compatibilityIssues"] = await GetCompatibilityIssuesAsync(schema1, schema2);
        
        return comparison;
    }

    // Private helper methods
    private async Task<TableSchema> InferSchemaFromSampleDataAsync(Guid fileId)
    {
        // Simulate schema inference from sample data
        return new TableSchema
        {
            Fields = new List<SchemaField>
            {
                new() { Name = "id", Type = "long", Nullable = false },
                new() { Name = "name", Type = "string", Nullable = true },
                new() { Name = "email", Type = "string", Nullable = true },
                new() { Name = "age", Type = "int", Nullable = true },
                new() { Name = "created_at", Type = "timestamp", Nullable = false },
                new() { Name = "is_active", Type = "boolean", Nullable = false }
            }
        };
    }

    private async Task<ColumnStatistics> GenerateColumnStatisticsAsync(Guid fileId, SchemaField field)
    {
        // Simulate statistics generation
        var random = new Random();
        
        return new ColumnStatistics
        {
            ColumnName = field.Name,
            DataType = field.Type,
            NullCount = field.Nullable ? random.Next(0, 1000) : 0,
            DistinctCount = random.Next(1, 10000),
            MinValue = field.Type switch
            {
                "int" or "long" => "0",
                "double" or "float" => "0.0",
                "string" => "A",
                "timestamp" => "2020-01-01T00:00:00Z",
                _ => null
            },
            MaxValue = field.Type switch
            {
                "int" or "long" => "1000000",
                "double" or "float" => "1000000.0",
                "string" => "zzz",
                "timestamp" => "2024-12-31T23:59:59Z",
                _ => null
            },
            MeanValue = field.Type.Contains("int") || field.Type.Contains("double") || field.Type.Contains("float") 
                ? random.NextDouble() * 1000 : null,
            StandardDeviation = field.Type.Contains("int") || field.Type.Contains("double") || field.Type.Contains("float") 
                ? random.NextDouble() * 100 : null
        };
    }

    private static TableSchema ConvertArrowSchemaToTableSchema(Dictionary<string, object> arrowSchema)
    {
        var schema = new TableSchema();
        
        if (arrowSchema.TryGetValue("fields", out var fieldsObj) && fieldsObj is JsonElement fieldsElement)
        {
            var fields = new List<SchemaField>();
            
            foreach (var fieldElement in fieldsElement.EnumerateArray())
            {
                if (fieldElement.TryGetProperty("name", out var nameElement) &&
                    fieldElement.TryGetProperty("type", out var typeElement))
                {
                    fields.Add(new SchemaField
                    {
                        Name = nameElement.GetString() ?? "unknown",
                        Type = ConvertArrowTypeToCommonType(typeElement.GetString() ?? "string"),
                        Nullable = fieldElement.TryGetProperty("nullable", out var nullableElement) 
                            ? nullableElement.GetBoolean() : true
                    });
                }
            }
            
            schema.Fields = fields;
        }
        
        return schema;
    }

    private static string ConvertArrowTypeToCommonType(string arrowType)
    {
        return arrowType.ToLower() switch
        {
            "int8" or "int16" or "int32" => "int",
            "int64" => "long",
            "float32" => "float",
            "float64" => "double",
            "utf8" or "string" => "string",
            "bool" => "boolean",
            "timestamp" => "timestamp",
            "date32" or "date64" => "date",
            _ => "string"
        };
    }

    private static bool IsValidFieldName(string name)
    {
        return Regex.IsMatch(name, @"^[a-zA-Z_][a-zA-Z0-9_]*$");
    }

    private static bool IsValidDataType(string type)
    {
        var validTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "boolean", "int", "long", "float", "double", "decimal", "string", 
            "binary", "date", "timestamp", "array", "map", "struct"
        };
        
        return validTypes.Contains(type);
    }

    private static bool AreTypesCompatible(string oldType, string newType)
    {
        if (oldType == newType) return true;
        
        // Define type promotion rules
        var promotions = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["int"] = new() { "long", "double", "string" },
            ["long"] = new() { "double", "string" },
            ["float"] = new() { "double", "string" },
            ["double"] = new() { "string" },
            ["boolean"] = new() { "string" },
            ["date"] = new() { "timestamp", "string" },
            ["timestamp"] = new() { "string" }
        };
        
        return promotions.TryGetValue(oldType, out var compatibleTypes) && 
               compatibleTypes.Contains(newType);
    }

    private static string GetPromotedType(string type1, string type2)
    {
        if (type1 == type2) return type1;
        
        // Type promotion hierarchy
        var hierarchy = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["boolean"] = 1,
            ["int"] = 2,
            ["long"] = 3,
            ["float"] = 4,
            ["double"] = 5,
            ["date"] = 6,
            ["timestamp"] = 7,
            ["string"] = 10
        };
        
        var level1 = hierarchy.GetValueOrDefault(type1, 0);
        var level2 = hierarchy.GetValueOrDefault(type2, 0);
        
        return level1 > level2 ? type1 : type2;
    }

    private static string GetTypeGroup(string type)
    {
        return type.ToLower() switch
        {
            "boolean" => "logical",
            "int" or "long" => "integer",
            "float" or "double" or "decimal" => "numeric",
            "string" => "text",
            "date" or "timestamp" => "temporal",
            "binary" => "binary",
            "array" or "map" or "struct" => "complex",
            _ => "other"
        };
    }
}