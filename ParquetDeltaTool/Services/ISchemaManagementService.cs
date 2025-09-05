using ParquetDeltaTool.Models;

namespace ParquetDeltaTool.Services;

public interface ISchemaManagementService
{
    // Schema Discovery and Analysis
    Task<TableSchema> InferSchemaFromDataAsync(Guid fileId);
    Task<TableSchema> ExtractSchemaFromParquetAsync(byte[] parquetData);
    Task<List<SchemaField>> GetColumnsAsync(Guid fileId);
    Task<Dictionary<string, ColumnStatistics>> GetColumnStatisticsAsync(Guid fileId);
    
    // Schema Validation and Compatibility
    Task<bool> ValidateSchemaAsync(TableSchema schema);
    Task<List<string>> GetSchemaValidationErrorsAsync(TableSchema schema);
    Task<bool> IsBackwardCompatibleAsync(TableSchema oldSchema, TableSchema newSchema);
    Task<List<string>> GetCompatibilityIssuesAsync(TableSchema oldSchema, TableSchema newSchema);
    
    // Schema Evolution and Management
    Task<TableSchema> MergeSchemaAsync(TableSchema schema1, TableSchema schema2);
    Task<TableSchema> AddColumnAsync(TableSchema schema, SchemaField newColumn);
    Task<TableSchema> DropColumnAsync(TableSchema schema, string columnName);
    Task<TableSchema> RenameColumnAsync(TableSchema schema, string oldName, string newName);
    Task<TableSchema> ChangeColumnTypeAsync(TableSchema schema, string columnName, string newType);
    
    // Schema Visualization and Documentation
    Task<string> GenerateSchemaJsonAsync(TableSchema schema);
    Task<string> GenerateSchemaDocumentationAsync(TableSchema schema);
    Task<Dictionary<string, object>> GetSchemaVisualizationDataAsync(TableSchema schema);
    
    // Schema History and Versioning
    Task<List<TableSchema>> GetSchemaHistoryAsync(Guid fileId);
    Task<TableSchema> GetSchemaAtVersionAsync(Guid fileId, long version);
    Task<Dictionary<string, object>> CompareSchemaVersionsAsync(TableSchema schema1, TableSchema schema2);
}