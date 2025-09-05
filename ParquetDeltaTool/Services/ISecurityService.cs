namespace ParquetDeltaTool.Services;

public interface ISecurityService
{
    // Content Validation
    Task<bool> ValidateFileContentAsync(byte[] fileContent, string fileName);
    Task<bool> ValidateUploadedFileAsync(Stream fileStream, string fileName);
    Task<List<string>> ScanForThreatsAsync(byte[] content);
    Task<bool> IsFileTypeAllowedAsync(string fileName, string contentType);
    
    // Input Sanitization
    Task<string> SanitizeFileNameAsync(string fileName);
    Task<string> SanitizeSqlQueryAsync(string sqlQuery);
    Task<Dictionary<string, string>> SanitizeMetadataAsync(Dictionary<string, string> metadata);
    Task<bool> ValidateColumnNamesAsync(List<string> columnNames);
    
    // Access Control
    Task<bool> ValidateOperationPermissionsAsync(string operation, string resourceId);
    Task<List<string>> GetAllowedOperationsAsync(string resourceId);
    Task<bool> IsRateLimitExceededAsync(string clientId, string operation);
    
    // Data Privacy
    Task<List<string>> DetectSensitiveDataAsync(Dictionary<string, object> data);
    Task<Dictionary<string, object>> RedactSensitiveDataAsync(Dictionary<string, object> data);
    Task<bool> RequiresDataMaskingAsync(string columnName, string dataType);
    
    // Security Headers and Configuration
    Task<Dictionary<string, string>> GetSecurityHeadersAsync();
    Task<bool> ValidateContentSecurityPolicyAsync(string cspPolicy);
    Task<SecurityConfiguration> GetSecurityConfigurationAsync();
    
    // Audit and Monitoring
    Task LogSecurityEventAsync(SecurityEvent securityEvent);
    Task<List<SecurityEvent>> GetSecurityEventsAsync(TimeSpan period);
    Task<Dictionary<string, object>> GetSecurityMetricsAsync();
    
    // Encryption and Hashing
    Task<string> HashSensitiveDataAsync(string data);
    Task<byte[]> EncryptDataAsync(byte[] data, string key);
    Task<byte[]> DecryptDataAsync(byte[] encryptedData, string key);
    Task<string> GenerateSecureTokenAsync();
}

public class SecurityConfiguration
{
    public List<string> AllowedFileTypes { get; set; } = new();
    public long MaxFileSizeBytes { get; set; } = 2L * 1024 * 1024 * 1024; // 2GB
    public int MaxConcurrentUploads { get; set; } = 5;
    public TimeSpan SessionTimeout { get; set; } = TimeSpan.FromHours(4);
    public bool EnableContentScanning { get; set; } = true;
    public bool RequireHttps { get; set; } = true;
    public Dictionary<string, int> RateLimits { get; set; } = new();
    public List<string> BlockedPatterns { get; set; } = new();
    public bool EnableAuditLogging { get; set; } = true;
}

public class SecurityEvent
{
    public string EventId { get; set; } = Guid.NewGuid().ToString();
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public string EventType { get; set; } = string.Empty;
    public string Severity { get; set; } = string.Empty;
    public string Source { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public string ResourceId { get; set; } = string.Empty;
    public Dictionary<string, object> AdditionalData { get; set; } = new();
}