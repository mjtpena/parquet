using ParquetDeltaTool.Models;
using Microsoft.JSInterop;
using System.Text.Json;

namespace ParquetDeltaTool.Services;

public class StorageService : IStorageService
{
    private readonly IJSRuntime _jsRuntime;
    private readonly ILogger<StorageService> _logger;
    private readonly Dictionary<Guid, FileMetadata> _memoryMetadata = new();
    private readonly Dictionary<Guid, byte[]> _memoryData = new();

    public StorageService(IJSRuntime jsRuntime, ILogger<StorageService> logger)
    {
        _jsRuntime = jsRuntime;
        _logger = logger;
    }

    public async Task<Guid> StoreFileAsync(byte[] data, FileMetadata metadata)
    {
        try
        {
            // For now, store in memory
            // TODO: Implement IndexedDB storage via JS interop
            _memoryData[metadata.FileId] = data;
            _memoryMetadata[metadata.FileId] = metadata;

            _logger.LogInformation("Stored file {FileId} ({Size} bytes)", metadata.FileId, data.Length);
            
            await Task.Delay(1); // Simulate async work
            return metadata.FileId;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to store file {FileId}", metadata.FileId);
            throw;
        }
    }

    public async Task<byte[]> GetFileDataAsync(Guid fileId)
    {
        if (_memoryData.TryGetValue(fileId, out var data))
        {
            await Task.Delay(1); // Simulate async work
            return data;
        }
        
        throw new ArgumentException($"File with ID {fileId} not found");
    }

    public async Task<FileMetadata?> GetMetadataAsync(Guid fileId)
    {
        await Task.Delay(1); // Simulate async work
        return _memoryMetadata.TryGetValue(fileId, out var metadata) ? metadata : null;
    }

    public async Task<List<FileMetadata>> GetRecentFilesAsync(int count = 20)
    {
        await Task.Delay(1); // Simulate async work
        return _memoryMetadata.Values
            .OrderByDescending(m => m.ModifiedAt)
            .Take(count)
            .ToList();
    }

    public async Task<bool> DeleteFileAsync(Guid fileId)
    {
        await Task.Delay(1); // Simulate async work
        var removed = _memoryData.Remove(fileId) & _memoryMetadata.Remove(fileId);
        
        if (removed)
        {
            _logger.LogInformation("Deleted file {FileId}", fileId);
        }
        
        return removed;
    }

    public async Task<long> GetStorageUsageAsync()
    {
        await Task.Delay(1); // Simulate async work
        return _memoryData.Values.Sum(data => data.Length);
    }

    public async Task CleanupOldFilesAsync(TimeSpan retention)
    {
        var cutoffTime = DateTime.UtcNow - retention;
        var oldFiles = _memoryMetadata.Values
            .Where(m => m.ModifiedAt < cutoffTime)
            .ToList();

        foreach (var file in oldFiles)
        {
            await DeleteFileAsync(file.FileId);
        }

        _logger.LogInformation("Cleaned up {Count} old files", oldFiles.Count);
    }

    public async Task<FileMetadata> GetFileMetadataAsync(Guid fileId)
    {
        if (_memoryMetadata.TryGetValue(fileId, out var metadata))
        {
            await Task.Delay(1); // Simulate async work
            return metadata;
        }
        
        throw new ArgumentException($"File metadata with ID {fileId} not found");
    }

    public async Task StoreFileMetadataAsync(FileMetadata metadata)
    {
        _memoryMetadata[metadata.FileId] = metadata;
        _logger.LogInformation("Stored metadata for file {FileId}", metadata.FileId);
        await Task.Delay(1); // Simulate async work
    }

    public async Task<List<FileMetadata>> GetAllFilesAsync()
    {
        await Task.Delay(1); // Simulate async work
        return _memoryMetadata.Values.ToList();
    }
}