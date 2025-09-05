using ParquetDeltaTool.Models;

namespace ParquetDeltaTool.Components.Layout;

// Supporting classes for UI components
public enum StatusType
{
    Info,
    Success,
    Warning,
    Error
}

public class PerformanceMetrics
{
    public double ExecutionTime { get; set; } // milliseconds
    public long MemoryUsage { get; set; } // bytes
    public double CpuUsage { get; set; } // percentage
    public int ActiveConnections { get; set; }
}

public class StatusBarViewState
{
    public int CurrentRow { get; set; }
    public int CurrentColumn { get; set; }
    public string ViewMode { get; set; } = "Table";
    public Dictionary<string, object> Settings { get; set; } = new();
}

// Classes for NavigatorPanel
public class DeltaVersion
{
    public long Version { get; set; }
    public DateTime CreatedAt { get; set; }
    public string Operation { get; set; } = string.Empty;
    public bool IsCurrent { get; set; }
}

public class Bookmark
{
    public string Name { get; set; } = string.Empty;
    public string Path { get; set; } = string.Empty;
    public string Icon { get; set; } = "bookmark";
}

// Classes for InspectorPanel
public class SmartInsight
{
    public InsightType Type { get; set; }
    public string Title { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public List<InsightAction>? Actions { get; set; }
}

public class InsightAction
{
    public string Label { get; set; } = string.Empty;
    public string Icon { get; set; } = "play_arrow";
    public string ActionType { get; set; } = string.Empty;
}

public enum InsightType
{
    Performance,
    DataQuality,
    Schema,
    Cost,
    Error
}

public class SelectedItemInfo
{
    public string Name { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
    public string Size { get; set; } = string.Empty;
    public DateTime ModifiedAt { get; set; }
    public Dictionary<string, string>? Metadata { get; set; }
}

public class DataStatistics
{
    public long RowCount { get; set; }
    public int ColumnCount { get; set; }
    public long DataSize { get; set; }
    public double CompressionRatio { get; set; }
    public List<ColumnStatistics>? ColumnStats { get; set; }
}

public class InspectorColumnStatistics
{
    public string ColumnName { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public double NullPercentage { get; set; }
    public long UniqueCount { get; set; }
}

public class InspectorValidationResult
{
    public bool IsValid { get; set; }
    public ValidationSeverity Severity { get; set; }
    public string RuleName { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public string Location { get; set; } = string.Empty;
}

public enum ValidationSeverity
{
    Success,
    Info,
    Warning,
    Error
}

public class HistoryItem
{
    public string Action { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
}

// Classes for CommandBar
public class CommandItem
{
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string Icon { get; set; } = "circle";
    public MudBlazor.Color IconColor { get; set; } = MudBlazor.Color.Default;
    public string Category { get; set; } = string.Empty;
    public string Action { get; set; } = string.Empty;
    public string Target { get; set; } = string.Empty;
    public string Shortcut { get; set; } = string.Empty;
}