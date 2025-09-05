using ParquetDeltaTool.Models;

namespace ParquetDeltaTool.State;

public class ApplicationState
{
    private readonly List<FileState> _openFiles = new();
    private readonly List<FileMetadata> _loadedFiles = new();
    public event EventHandler? StateChanged;

    public FileState? ActiveFile { get; private set; }
    public IReadOnlyList<FileState> OpenFiles => _openFiles.AsReadOnly();
    public List<FileMetadata> LoadedFiles => _loadedFiles;
    public UserPreferences Preferences { get; set; } = new();
    
    public Action OnChange => () => StateChanged?.Invoke(this, EventArgs.Empty);

    public void OpenFile(FileMetadata metadata)
    {
        var existing = _openFiles.FirstOrDefault(f => f.Metadata.FileId == metadata.FileId);
        if (existing != null)
        {
            ActiveFile = existing;
            NotifyStateChanged();
            return;
        }

        var fileState = new FileState
        {
            Id = Guid.NewGuid(),
            Metadata = metadata,
            OpenedAt = DateTime.UtcNow,
            ViewState = new ViewState()
        };

        _openFiles.Add(fileState);
        ActiveFile = fileState;
        NotifyStateChanged();
    }

    public void CloseFile(Guid fileId)
    {
        var file = _openFiles.FirstOrDefault(f => f.Id == fileId);
        if (file != null)
        {
            _openFiles.Remove(file);
            if (ActiveFile == file)
            {
                ActiveFile = _openFiles.FirstOrDefault();
            }
            NotifyStateChanged();
        }
    }

    public void SetActiveFile(Guid fileId)
    {
        var file = _openFiles.FirstOrDefault(f => f.Id == fileId);
        if (file != null)
        {
            ActiveFile = file;
            NotifyStateChanged();
        }
    }

    public void UpdateViewState(Guid fileId, Action<ViewState> update)
    {
        var file = _openFiles.FirstOrDefault(f => f.Id == fileId);
        if (file != null)
        {
            update(file.ViewState);
            NotifyStateChanged();
        }
    }

    private void NotifyStateChanged() => StateChanged?.Invoke(this, EventArgs.Empty);
}

public class FileState
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public FileMetadata Metadata { get; set; } = null!;
    public DateTime OpenedAt { get; set; }
    public ViewState ViewState { get; set; } = new();
    public List<QueryResult> QueryHistory { get; set; } = new();
}

public class ViewState
{
    public int ScrollTop { get; set; }
    public int ScrollLeft { get; set; }
    public List<string> VisibleColumns { get; set; } = new();
    public Dictionary<string, SortDirection> SortColumns { get; set; } = new();
    public List<Filter> ActiveFilters { get; set; } = new();
    public ViewMode Mode { get; set; } = ViewMode.DataPreview;
    public Dictionary<string, bool> ExpandedNodes { get; set; } = new();
    public int PreviewRows { get; set; } = 100;
    public int PreviewOffset { get; set; } = 0;
}

public class UserPreferences
{
    public Theme Theme { get; set; } = Theme.Auto;
    public LayoutMode Layout { get; set; } = LayoutMode.ThreePanel;
    public int DefaultPreviewRows { get; set; } = 100;
    public bool ShowAdvancedFeatures { get; set; } = false;
    public List<string> PinnedTools { get; set; } = new();
    public Dictionary<string, object> CustomSettings { get; set; } = new();
}

public class Filter
{
    public string Column { get; set; } = string.Empty;
    public DataFilterOperator Operator { get; set; }
    public object? Value { get; set; }
    public bool IsActive { get; set; } = true;
}

public enum SortDirection
{
    Ascending,
    Descending
}

public enum ViewMode
{
    DataPreview,
    Schema,
    Query,
    Statistics,
    Metadata
}

public enum Theme
{
    Light,
    Dark,
    Auto
}

public enum LayoutMode
{
    SinglePanel,
    TwoPanel,
    ThreePanel
}

public enum DataFilterOperator
{
    Equals,
    NotEquals,
    Contains,
    StartsWith,
    EndsWith,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    IsNull,
    IsNotNull
}