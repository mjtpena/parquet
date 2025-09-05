using ParquetDeltaTool.Models;

namespace ParquetDeltaTool.Services;

public interface IAdvancedAnalyticsService
{
    // Advanced Statistics
    Task<FileStatistics> CalculateAdvancedStatisticsAsync(Guid fileId);
    Task<Dictionary<string, object>> GetDataProfileAsync(Guid fileId);
    Task<List<DataQualityIssue>> AnalyzeDataQualityAsync(Guid fileId);
    Task<Dictionary<string, double>> CalculateCorrelationMatrixAsync(Guid fileId, List<string> numericColumns);
    
    // Performance Analysis
    Task<List<PerformanceInsight>> AnalyzePerformanceAsync(Guid fileId);
    Task<Dictionary<string, object>> GetStorageAnalysisAsync(Guid fileId);
    Task<Dictionary<string, object>> GetCompressionAnalysisAsync(Guid fileId);
    Task<List<string>> GetOptimizationRecommendationsAsync(Guid fileId);
    
    // Query Performance
    Task<QueryExecutionPlan> AnalyzeQueryPlanAsync(string sqlQuery, Guid fileId);
    Task<Dictionary<string, object>> GetQueryPerformanceMetricsAsync(string queryId);
    Task<List<string>> SuggestQueryOptimizationsAsync(string sqlQuery);
    Task<Dictionary<string, long>> GetQueryExecutionStatsAsync(Guid fileId);
    
    // Data Distribution Analysis
    Task<Dictionary<string, object>> AnalyzeDataDistributionAsync(Guid fileId, string columnName);
    Task<List<object>> GetTopValuesAsync(Guid fileId, string columnName, int limit = 10);
    Task<Dictionary<string, object>> DetectOutliersAsync(Guid fileId, string columnName);
    Task<Dictionary<string, double>> GetPercentileStatisticsAsync(Guid fileId, string columnName);
    
    // Partitioning Analysis
    Task<Dictionary<string, object>> AnalyzePartitioningAsync(Guid fileId);
    Task<List<string>> SuggestPartitioningStrategyAsync(Guid fileId);
    Task<Dictionary<string, object>> GetPartitionStatisticsAsync(Guid fileId);
    
    // Data Lineage and Impact Analysis
    Task<Dictionary<string, object>> GetDataLineageAsync(Guid fileId);
    Task<List<Guid>> GetDependentTablesAsync(Guid fileId);
    Task<Dictionary<string, object>> AnalyzeSchemaImpactAsync(Guid fileId, TableSchema newSchema);
}