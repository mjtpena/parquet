using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace ParquetDeltaTool.Services;

public class SecurityService : ISecurityService
{
    private readonly ILogger<SecurityService> _logger;
    private readonly SecurityConfiguration _config;
    private readonly Dictionary<string, List<DateTime>> _rateLimitTracker = new();
    private readonly List<SecurityEvent> _securityEvents = new();

    public SecurityService(ILogger<SecurityService> logger)
    {
        _logger = logger;
        _config = new SecurityConfiguration
        {
            AllowedFileTypes = new List<string> { ".parquet", ".csv", ".json", ".jsonl", ".avro", ".orc" },
            MaxFileSizeBytes = 2L * 1024 * 1024 * 1024, // 2GB
            MaxConcurrentUploads = 5,
            EnableContentScanning = true,
            RequireHttps = true,
            RateLimits = new Dictionary<string, int>
            {
                ["upload"] = 10, // 10 uploads per hour
                ["export"] = 50, // 50 exports per hour
                ["query"] = 100  // 100 queries per hour
            },
            BlockedPatterns = new List<string>
            {
                @"<script.*?>.*?</script>", // XSS patterns
                @"javascript:", // JavaScript protocols
                @"on\w+\s*=", // Event handlers
                @"eval\s*\(", // Eval calls
                @"exec\s*\(" // Exec calls
            }
        };
    }

    public async Task<bool> ValidateFileContentAsync(byte[] fileContent, string fileName)
    {
        _logger.LogInformation("Validating file content for {FileName}", fileName);

        try
        {
            // Check file size
            if (fileContent.Length > _config.MaxFileSizeBytes)
            {
                await LogSecurityEventAsync(new SecurityEvent
                {
                    EventType = "FILE_TOO_LARGE",
                    Severity = "MEDIUM",
                    Description = $"File {fileName} exceeds size limit ({fileContent.Length} bytes)",
                    ResourceId = fileName
                });
                return false;
            }

            // Check file type by extension
            if (!await IsFileTypeAllowedAsync(fileName, ""))
            {
                return false;
            }

            // Scan for threats if enabled
            if (_config.EnableContentScanning)
            {
                var threats = await ScanForThreatsAsync(fileContent);
                if (threats.Any())
                {
                    await LogSecurityEventAsync(new SecurityEvent
                    {
                        EventType = "THREAT_DETECTED",
                        Severity = "HIGH",
                        Description = $"Threats detected in {fileName}: {string.Join(", ", threats)}",
                        ResourceId = fileName,
                        AdditionalData = new Dictionary<string, object> { ["threats"] = threats }
                    });
                    return false;
                }
            }

            // Validate file signatures (magic numbers)
            if (!ValidateFileSignature(fileContent, fileName))
            {
                await LogSecurityEventAsync(new SecurityEvent
                {
                    EventType = "INVALID_FILE_SIGNATURE",
                    Severity = "MEDIUM",
                    Description = $"File {fileName} has invalid or missing file signature",
                    ResourceId = fileName
                });
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error validating file content for {FileName}", fileName);
            await LogSecurityEventAsync(new SecurityEvent
            {
                EventType = "VALIDATION_ERROR",
                Severity = "HIGH",
                Description = $"Error validating file {fileName}: {ex.Message}",
                ResourceId = fileName
            });
            return false;
        }
    }

    public async Task<bool> ValidateUploadedFileAsync(Stream fileStream, string fileName)
    {
        using var memoryStream = new MemoryStream();
        await fileStream.CopyToAsync(memoryStream);
        var fileContent = memoryStream.ToArray();
        
        return await ValidateFileContentAsync(fileContent, fileName);
    }

    public async Task<List<string>> ScanForThreatsAsync(byte[] content)
    {
        var threats = new List<string>();
        var contentString = Encoding.UTF8.GetString(content);

        // Check for blocked patterns
        foreach (var pattern in _config.BlockedPatterns)
        {
            if (Regex.IsMatch(contentString, pattern, RegexOptions.IgnoreCase))
            {
                threats.Add($"Blocked pattern detected: {pattern}");
            }
        }

        // Check for suspicious file headers
        if (content.Length >= 4)
        {
            var header = Encoding.ASCII.GetString(content.Take(4).ToArray());
            if (header.StartsWith("MZ") || header.StartsWith("PK")) // Windows executables or compressed files
            {
                var fileName = Path.GetExtension(contentString) ?? "";
                if (!_config.AllowedFileTypes.Contains(fileName.ToLower()))
                {
                    threats.Add("Suspicious executable or compressed file header");
                }
            }
        }

        // Check for embedded scripts or malicious content
        var suspiciousKeywords = new[] { "eval(", "exec(", "system(", "shell_exec", "cmd.exe", "powershell" };
        foreach (var keyword in suspiciousKeywords)
        {
            if (contentString.Contains(keyword, StringComparison.OrdinalIgnoreCase))
            {
                threats.Add($"Suspicious keyword detected: {keyword}");
            }
        }

        return threats;
    }

    public async Task<bool> IsFileTypeAllowedAsync(string fileName, string contentType)
    {
        var extension = Path.GetExtension(fileName).ToLower();
        var isAllowed = _config.AllowedFileTypes.Contains(extension);

        if (!isAllowed)
        {
            await LogSecurityEventAsync(new SecurityEvent
            {
                EventType = "BLOCKED_FILE_TYPE",
                Severity = "LOW",
                Description = $"Blocked file type: {extension}",
                ResourceId = fileName
            });
        }

        return isAllowed;
    }

    public async Task<string> SanitizeFileNameAsync(string fileName)
    {
        if (string.IsNullOrEmpty(fileName))
            return "unnamed_file";

        // Remove path traversal attempts
        fileName = Path.GetFileName(fileName);

        // Remove or replace dangerous characters
        var dangerous = new char[] { '<', '>', ':', '"', '|', '?', '*', '\0' };
        foreach (var c in dangerous)
        {
            fileName = fileName.Replace(c, '_');
        }

        // Remove leading/trailing dots and spaces
        fileName = fileName.Trim(' ', '.');

        // Limit length
        if (fileName.Length > 255)
        {
            var extension = Path.GetExtension(fileName);
            var nameWithoutExt = Path.GetFileNameWithoutExtension(fileName);
            fileName = nameWithoutExt[..(255 - extension.Length)] + extension;
        }

        // Ensure it's not empty after sanitization
        if (string.IsNullOrEmpty(fileName))
            fileName = "sanitized_file";

        return fileName;
    }

    public async Task<string> SanitizeSqlQueryAsync(string sqlQuery)
    {
        if (string.IsNullOrEmpty(sqlQuery))
            return string.Empty;

        // Remove comments
        sqlQuery = Regex.Replace(sqlQuery, @"--.*$", "", RegexOptions.Multiline);
        sqlQuery = Regex.Replace(sqlQuery, @"/\*.*?\*/", "", RegexOptions.Singleline);

        // Block dangerous SQL keywords
        var blockedKeywords = new[] 
        { 
            "DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE", "INSERT", "UPDATE", 
            "GRANT", "REVOKE", "EXEC", "EXECUTE", "xp_", "sp_", "OPENROWSET", 
            "OPENDATASOURCE", "BULK", "SHUTDOWN", "BACKUP", "RESTORE"
        };

        foreach (var keyword in blockedKeywords)
        {
            var pattern = $@"\b{keyword}\b";
            if (Regex.IsMatch(sqlQuery, pattern, RegexOptions.IgnoreCase))
            {
                await LogSecurityEventAsync(new SecurityEvent
                {
                    EventType = "BLOCKED_SQL_KEYWORD",
                    Severity = "HIGH",
                    Description = $"Blocked SQL keyword detected: {keyword}",
                    AdditionalData = new Dictionary<string, object> { ["query"] = sqlQuery }
                });
                
                // Replace with safe placeholder or remove
                sqlQuery = Regex.Replace(sqlQuery, pattern, "/* BLOCKED */", RegexOptions.IgnoreCase);
            }
        }

        // Limit query length
        if (sqlQuery.Length > 10000)
        {
            sqlQuery = sqlQuery[..10000] + "/* TRUNCATED */";
        }

        return sqlQuery.Trim();
    }

    public async Task<Dictionary<string, string>> SanitizeMetadataAsync(Dictionary<string, string> metadata)
    {
        var sanitized = new Dictionary<string, string>();

        foreach (var kvp in metadata)
        {
            var key = await SanitizeStringAsync(kvp.Key);
            var value = await SanitizeStringAsync(kvp.Value);
            
            // Skip empty keys
            if (!string.IsNullOrEmpty(key))
            {
                sanitized[key] = value;
            }
        }

        return sanitized;
    }

    public async Task<bool> ValidateColumnNamesAsync(List<string> columnNames)
    {
        var validNamePattern = @"^[a-zA-Z_][a-zA-Z0-9_]*$";
        var reservedWords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
            "TABLE", "INDEX", "VIEW", "DATABASE", "SCHEMA", "COLUMN", "PRIMARY", "FOREIGN", "KEY"
        };

        foreach (var columnName in columnNames)
        {
            if (string.IsNullOrEmpty(columnName) || 
                !Regex.IsMatch(columnName, validNamePattern) ||
                reservedWords.Contains(columnName))
            {
                await LogSecurityEventAsync(new SecurityEvent
                {
                    EventType = "INVALID_COLUMN_NAME",
                    Severity = "MEDIUM",
                    Description = $"Invalid column name: {columnName}"
                });
                return false;
            }
        }

        return true;
    }

    public async Task<bool> ValidateOperationPermissionsAsync(string operation, string resourceId)
    {
        // Simple permission validation - in production, this would integrate with proper auth
        var allowedOperations = new HashSet<string>
        {
            "read", "upload", "export", "query", "analyze", "validate"
        };

        var isAllowed = allowedOperations.Contains(operation.ToLower());
        
        if (!isAllowed)
        {
            await LogSecurityEventAsync(new SecurityEvent
            {
                EventType = "UNAUTHORIZED_OPERATION",
                Severity = "HIGH",
                Description = $"Unauthorized operation attempted: {operation}",
                ResourceId = resourceId
            });
        }

        return isAllowed;
    }

    public async Task<List<string>> GetAllowedOperationsAsync(string resourceId)
    {
        // Return allowed operations based on resource and user context
        return new List<string> { "read", "upload", "export", "query", "analyze", "validate" };
    }

    public async Task<bool> IsRateLimitExceededAsync(string clientId, string operation)
    {
        if (!_config.RateLimits.TryGetValue(operation.ToLower(), out var limit))
        {
            return false; // No rate limit defined for this operation
        }

        var key = $"{clientId}:{operation}";
        var now = DateTime.UtcNow;
        var windowStart = now.AddHours(-1);

        if (!_rateLimitTracker.ContainsKey(key))
        {
            _rateLimitTracker[key] = new List<DateTime>();
        }

        var requests = _rateLimitTracker[key];
        
        // Clean old requests outside the window
        requests.RemoveAll(r => r < windowStart);
        
        if (requests.Count >= limit)
        {
            await LogSecurityEventAsync(new SecurityEvent
            {
                EventType = "RATE_LIMIT_EXCEEDED",
                Severity = "MEDIUM",
                Description = $"Rate limit exceeded for {operation} by client {clientId}",
                ClientId = clientId,
                AdditionalData = new Dictionary<string, object>
                {
                    ["operation"] = operation,
                    ["limit"] = limit,
                    ["requests"] = requests.Count
                }
            });
            return true;
        }

        requests.Add(now);
        return false;
    }

    public async Task<List<string>> DetectSensitiveDataAsync(Dictionary<string, object> data)
    {
        var sensitiveFields = new List<string>();
        var sensitivePatterns = new Dictionary<string, string>
        {
            ["email"] = @"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
            ["ssn"] = @"\b\d{3}-?\d{2}-?\d{4}\b",
            ["credit_card"] = @"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
            ["phone"] = @"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b",
            ["ip_address"] = @"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b"
        };

        foreach (var kvp in data)
        {
            var value = kvp.Value?.ToString() ?? "";
            
            // Check field name for sensitive keywords
            var fieldNameLower = kvp.Key.ToLower();
            if (fieldNameLower.Contains("password") || fieldNameLower.Contains("ssn") || 
                fieldNameLower.Contains("credit") || fieldNameLower.Contains("secret"))
            {
                sensitiveFields.Add($"Sensitive field name: {kvp.Key}");
            }

            // Check value patterns
            foreach (var pattern in sensitivePatterns)
            {
                if (Regex.IsMatch(value, pattern.Value, RegexOptions.IgnoreCase))
                {
                    sensitiveFields.Add($"Potential {pattern.Key} in field {kvp.Key}");
                }
            }
        }

        return sensitiveFields;
    }

    public async Task<Dictionary<string, object>> RedactSensitiveDataAsync(Dictionary<string, object> data)
    {
        var redacted = new Dictionary<string, object>();
        var sensitiveKeywords = new[] { "password", "ssn", "credit", "secret", "token" };

        foreach (var kvp in data)
        {
            var key = kvp.Key;
            var value = kvp.Value;

            // Check if field name contains sensitive keywords
            if (sensitiveKeywords.Any(keyword => key.ToLower().Contains(keyword)))
            {
                redacted[key] = "***REDACTED***";
            }
            else
            {
                // Redact common patterns in values
                var stringValue = value?.ToString() ?? "";
                
                // Redact email patterns
                stringValue = Regex.Replace(stringValue, @"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", 
                    "***@***.***", RegexOptions.IgnoreCase);
                
                // Redact SSN patterns
                stringValue = Regex.Replace(stringValue, @"\b\d{3}-?\d{2}-?\d{4}\b", "***-**-****");
                
                // Redact credit card patterns
                stringValue = Regex.Replace(stringValue, @"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b", 
                    "****-****-****-****");
                
                redacted[key] = stringValue == value?.ToString() ? value : stringValue;
            }
        }

        return redacted;
    }

    public async Task<bool> RequiresDataMaskingAsync(string columnName, string dataType)
    {
        var sensitiveColumnPatterns = new[]
        {
            "email", "phone", "ssn", "social_security", "credit_card", "password", 
            "secret", "token", "api_key", "address", "zip_code", "postal_code"
        };

        return sensitiveColumnPatterns.Any(pattern => 
            columnName.ToLower().Contains(pattern));
    }

    public async Task<Dictionary<string, string>> GetSecurityHeadersAsync()
    {
        return new Dictionary<string, string>
        {
            ["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains",
            ["X-Content-Type-Options"] = "nosniff",
            ["X-Frame-Options"] = "DENY",
            ["X-XSS-Protection"] = "1; mode=block",
            ["Referrer-Policy"] = "strict-origin-when-cross-origin",
            ["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()",
            ["Content-Security-Policy"] = "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' https://unpkg.com https://cdn.jsdelivr.net; style-src 'self' 'unsafe-inline'; font-src 'self' data:; img-src 'self' data: https:; connect-src 'self' https:; frame-ancestors 'none';",
            ["X-Permitted-Cross-Domain-Policies"] = "none",
            ["Clear-Site-Data"] = "\"cache\", \"cookies\", \"storage\", \"executionContexts\""
        };
    }

    public async Task<bool> ValidateContentSecurityPolicyAsync(string cspPolicy)
    {
        if (string.IsNullOrEmpty(cspPolicy))
            return false;

        // Basic CSP validation
        var requiredDirectives = new[] { "default-src", "script-src", "style-src" };
        var hasRequired = requiredDirectives.All(directive => 
            cspPolicy.Contains(directive, StringComparison.OrdinalIgnoreCase));

        // Check for unsafe practices
        var unsafePractices = new[] { "'unsafe-inline'", "'unsafe-eval'", "*" };
        var hasUnsafe = unsafePractices.Any(practice => 
            cspPolicy.Contains(practice, StringComparison.OrdinalIgnoreCase));

        if (hasUnsafe)
        {
            await LogSecurityEventAsync(new SecurityEvent
            {
                EventType = "UNSAFE_CSP_DETECTED",
                Severity = "MEDIUM",
                Description = "Content Security Policy contains potentially unsafe directives",
                AdditionalData = new Dictionary<string, object> { ["csp"] = cspPolicy }
            });
        }

        return hasRequired;
    }

    public async Task<SecurityConfiguration> GetSecurityConfigurationAsync()
    {
        return _config;
    }

    public async Task LogSecurityEventAsync(SecurityEvent securityEvent)
    {
        _securityEvents.Add(securityEvent);
        _logger.LogWarning("Security Event: {EventType} - {Description}", 
            securityEvent.EventType, securityEvent.Description);

        // In production, this would also:
        // - Send to SIEM system
        // - Alert security team for HIGH severity events
        // - Update security metrics
        // - Persist to database
    }

    public async Task<List<SecurityEvent>> GetSecurityEventsAsync(TimeSpan period)
    {
        var cutoff = DateTime.UtcNow - period;
        return _securityEvents.Where(e => e.Timestamp >= cutoff)
                             .OrderByDescending(e => e.Timestamp)
                             .ToList();
    }

    public async Task<Dictionary<string, object>> GetSecurityMetricsAsync()
    {
        var last24Hours = DateTime.UtcNow.AddHours(-24);
        var recentEvents = _securityEvents.Where(e => e.Timestamp >= last24Hours).ToList();

        return new Dictionary<string, object>
        {
            ["totalEvents"] = _securityEvents.Count,
            ["eventsLast24Hours"] = recentEvents.Count,
            ["eventsByType"] = recentEvents.GroupBy(e => e.EventType)
                                          .ToDictionary(g => g.Key, g => g.Count()),
            ["eventsBySeverity"] = recentEvents.GroupBy(e => e.Severity)
                                              .ToDictionary(g => g.Key, g => g.Count()),
            ["rateLimitViolations"] = recentEvents.Count(e => e.EventType == "RATE_LIMIT_EXCEEDED"),
            ["threatsDetected"] = recentEvents.Count(e => e.EventType == "THREAT_DETECTED"),
            ["unauthorizedAttempts"] = recentEvents.Count(e => e.EventType == "UNAUTHORIZED_OPERATION"),
            ["blockedFileTypes"] = recentEvents.Count(e => e.EventType == "BLOCKED_FILE_TYPE")
        };
    }

    public async Task<string> HashSensitiveDataAsync(string data)
    {
        if (string.IsNullOrEmpty(data))
            return string.Empty;

        using var sha256 = SHA256.Create();
        var hashedBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(data));
        return Convert.ToBase64String(hashedBytes);
    }

    public async Task<byte[]> EncryptDataAsync(byte[] data, string key)
    {
        using var aes = Aes.Create();
        aes.Key = Encoding.UTF8.GetBytes(key.PadRight(32).Substring(0, 32)); // Ensure 32-byte key
        aes.GenerateIV();

        using var encryptor = aes.CreateEncryptor();
        using var msEncrypt = new MemoryStream();
        
        // Prepend IV to encrypted data
        msEncrypt.Write(aes.IV, 0, aes.IV.Length);
        
        using var csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write);
        csEncrypt.Write(data, 0, data.Length);
        csEncrypt.FlushFinalBlock();
        
        return msEncrypt.ToArray();
    }

    public async Task<byte[]> DecryptDataAsync(byte[] encryptedData, string key)
    {
        using var aes = Aes.Create();
        aes.Key = Encoding.UTF8.GetBytes(key.PadRight(32).Substring(0, 32)); // Ensure 32-byte key
        
        // Extract IV from beginning of encrypted data
        var iv = new byte[16];
        Array.Copy(encryptedData, 0, iv, 0, 16);
        aes.IV = iv;
        
        using var decryptor = aes.CreateDecryptor();
        using var msDecrypt = new MemoryStream(encryptedData, 16, encryptedData.Length - 16);
        using var csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read);
        using var msResult = new MemoryStream();
        
        csDecrypt.CopyTo(msResult);
        return msResult.ToArray();
    }

    public async Task<string> GenerateSecureTokenAsync()
    {
        var bytes = new byte[32];
        using var rng = RandomNumberGenerator.Create();
        rng.GetBytes(bytes);
        return Convert.ToBase64String(bytes).Replace("+", "-").Replace("/", "_").TrimEnd('=');
    }

    // Private helper methods
    private async Task<string> SanitizeStringAsync(string input)
    {
        if (string.IsNullOrEmpty(input))
            return string.Empty;

        // Remove control characters
        input = Regex.Replace(input, @"[\x00-\x1F\x7F]", "");
        
        // Remove or encode HTML/XML entities
        input = input.Replace("<", "&lt;").Replace(">", "&gt;").Replace("\"", "&quot;");
        
        // Trim and limit length
        input = input.Trim();
        if (input.Length > 1000)
        {
            input = input[..1000];
        }

        return input;
    }

    private bool ValidateFileSignature(byte[] content, string fileName)
    {
        if (content.Length < 4) return false;

        var extension = Path.GetExtension(fileName).ToLower();
        var header = content.Take(8).ToArray();

        return extension switch
        {
            ".parquet" => header.Take(4).SequenceEqual(Encoding.ASCII.GetBytes("PAR1")),
            ".csv" => true, // CSV has no magic number
            ".json" => true, // JSON has no magic number
            ".jsonl" => true, // JSONL has no magic number
            ".avro" => header.Take(4).SequenceEqual(new byte[] { 0x4F, 0x62, 0x6A, 0x01 }),
            ".orc" => header.Take(3).SequenceEqual(Encoding.ASCII.GetBytes("ORC")),
            _ => false
        };
    }
}