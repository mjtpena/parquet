using ParquetDeltaTool.Models;

namespace ParquetDeltaTool.Testing;

public interface ITestingService
{
    // Test Execution
    Task<TestResult> RunTestSuiteAsync(string suiteName);
    Task<TestResult> RunTestAsync(string testName);
    Task<List<TestResult>> RunAllTestsAsync();
    Task<TestResult> RunDataValidationTestsAsync(Guid fileId);
    
    // Test Management
    Task<List<TestSuite>> GetAvailableTestSuitesAsync();
    Task<TestSuite> GetTestSuiteAsync(string suiteName);
    Task<List<TestCase>> GetTestCasesAsync(string suiteName);
    Task RegisterTestSuiteAsync(TestSuite testSuite);
    
    // Data Quality Testing
    Task<TestResult> ValidateSchemaAsync(Guid fileId, TableSchema expectedSchema);
    Task<TestResult> ValidateDataIntegrityAsync(Guid fileId);
    Task<TestResult> ValidateDataQualityAsync(Guid fileId, DataQualityRules rules);
    Task<TestResult> ValidatePerformanceAsync(Guid fileId, PerformanceBenchmarks benchmarks);
    
    // Integration Testing
    Task<TestResult> TestFileUploadWorkflowAsync();
    Task<TestResult> TestDataExportWorkflowAsync(Guid fileId, FileFormat format);
    Task<TestResult> TestDeltaLakeOperationsAsync(Guid fileId);
    Task<TestResult> TestQueryExecutionAsync(string sqlQuery, Guid fileId);
    
    // Load Testing
    Task<TestResult> RunLoadTestAsync(LoadTestConfiguration config);
    Task<TestResult> TestConcurrentOperationsAsync(int concurrentUsers, TimeSpan duration);
    Task<TestResult> TestLargeDatasetHandlingAsync(long fileSizeBytes);
    
    // Regression Testing
    Task<TestResult> RunRegressionTestsAsync(string baselineVersion);
    Task<TestResult> ComparePerformanceAsync(string baselineVersion, string currentVersion);
    Task SavePerformanceBaselineAsync(string version);
    
    // Test Reporting
    Task<TestReport> GenerateTestReportAsync(List<TestResult> results);
    Task<string> ExportTestReportAsync(TestReport report, ReportFormat format);
    Task<Dictionary<string, object>> GetTestMetricsAsync();
    Task<List<TestHistory>> GetTestHistoryAsync(TimeSpan period);
}

public class TestResult
{
    public string TestName { get; set; } = string.Empty;
    public string TestSuite { get; set; } = string.Empty;
    public TestStatus Status { get; set; }
    public TimeSpan Duration { get; set; }
    public DateTime ExecutedAt { get; set; } = DateTime.UtcNow;
    public string? ErrorMessage { get; set; }
    public string? StackTrace { get; set; }
    public Dictionary<string, object> Metrics { get; set; } = new();
    public List<TestAssertion> Assertions { get; set; } = new();
    public Dictionary<string, object> TestData { get; set; } = new();
}

public class TestSuite
{
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public List<TestCase> TestCases { get; set; } = new();
    public Dictionary<string, object> Configuration { get; set; } = new();
    public TimeSpan Timeout { get; set; } = TimeSpan.FromMinutes(5);
}

public class TestCase
{
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public TestType Type { get; set; }
    public Dictionary<string, object> Parameters { get; set; } = new();
    public List<TestAssertion> ExpectedAssertions { get; set; } = new();
    public TimeSpan Timeout { get; set; } = TimeSpan.FromMinutes(1);
    public bool Enabled { get; set; } = true;
}

public class TestAssertion
{
    public string Name { get; set; } = string.Empty;
    public AssertionType Type { get; set; }
    public object? Expected { get; set; }
    public object? Actual { get; set; }
    public bool Passed { get; set; }
    public string? Message { get; set; }
}

public class DataQualityRules
{
    public Dictionary<string, ColumnValidationRule> ColumnRules { get; set; } = new();
    public List<CrossColumnRule> CrossColumnRules { get; set; } = new();
    public double MinCompletenessThreshold { get; set; } = 0.95;
    public double MinValidityThreshold { get; set; } = 0.98;
    public int MaxDuplicatePercentage { get; set; } = 5;
}

public class ColumnValidationRule
{
    public string ColumnName { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsRequired { get; set; }
    public object? MinValue { get; set; }
    public object? MaxValue { get; set; }
    public List<object>? AllowedValues { get; set; }
    public string? Pattern { get; set; }
    public double? MaxNullPercentage { get; set; }
}

public class CrossColumnRule
{
    public string Name { get; set; } = string.Empty;
    public string Rule { get; set; } = string.Empty;
    public List<string> Columns { get; set; } = new();
    public string ErrorMessage { get; set; } = string.Empty;
}

public class PerformanceBenchmarks
{
    public TimeSpan MaxQueryTime { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan MaxExportTime { get; set; } = TimeSpan.FromMinutes(5);
    public long MaxMemoryUsageBytes { get; set; } = 1024 * 1024 * 1024; // 1GB
    public double MinThroughputMBps { get; set; } = 10;
    public int MaxConcurrentUsers { get; set; } = 100;
}

public class LoadTestConfiguration
{
    public int ConcurrentUsers { get; set; } = 10;
    public TimeSpan Duration { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan RampUpTime { get; set; } = TimeSpan.FromMinutes(1);
    public List<string> TestScenarios { get; set; } = new();
    public Dictionary<string, object> Parameters { get; set; } = new();
}

public class TestReport
{
    public string ReportId { get; set; } = Guid.NewGuid().ToString();
    public DateTime GeneratedAt { get; set; } = DateTime.UtcNow;
    public string Version { get; set; } = string.Empty;
    public TestSummary Summary { get; set; } = new();
    public List<TestResult> Results { get; set; } = new();
    public Dictionary<string, object> Metrics { get; set; } = new();
    public List<string> Recommendations { get; set; } = new();
}

public class TestSummary
{
    public int TotalTests { get; set; }
    public int PassedTests { get; set; }
    public int FailedTests { get; set; }
    public int SkippedTests { get; set; }
    public TimeSpan TotalDuration { get; set; }
    public double PassRate => TotalTests > 0 ? (double)PassedTests / TotalTests : 0;
}

public class TestHistory
{
    public DateTime Date { get; set; }
    public TestSummary Summary { get; set; } = new();
    public Dictionary<string, double> Metrics { get; set; } = new();
}

public enum TestStatus
{
    NotRun,
    Running,
    Passed,
    Failed,
    Skipped,
    Error
}

public enum TestType
{
    Unit,
    Integration,
    Performance,
    Load,
    DataQuality,
    Regression,
    Security
}

public enum AssertionType
{
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    Contains,
    NotNull,
    Null,
    True,
    False,
    Matches
}

public enum ReportFormat
{
    Json,
    Html,
    Xml,
    Csv
}