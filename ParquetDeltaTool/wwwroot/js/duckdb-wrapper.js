// DuckDB WASM Wrapper
// This is a mock implementation - in production, you would use actual DuckDB WASM bindings

class DuckDBWrapper {
    constructor() {
        this.initialized = false;
        this.registeredFiles = new Map();
    }

    async initialize() {
        if (!this.initialized) {
            console.log('Initializing DuckDB WASM...');
            // TODO: Initialize actual DuckDB WASM
            await new Promise(resolve => setTimeout(resolve, 200));
            this.initialized = true;
        }
        return this.initialized;
    }

    async registerParquetFile(fileId, bytes) {
        await this.initialize();
        
        console.log(`Registering Parquet file ${fileId} (${bytes.length} bytes)`);
        
        // Store file reference for queries
        this.registeredFiles.set(fileId, {
            bytes,
            tableName: `file_${fileId.replace('-', '_')}`
        });
        
        console.log(`File registered as table: file_${fileId.replace('-', '_')}`);
    }

    async executeQuery(sql, maxRows = 10000) {
        await this.initialize();
        
        console.log(`Executing query: ${sql}`);
        
        const startTime = performance.now();
        
        // Simulate query execution time
        await new Promise(resolve => setTimeout(resolve, Math.random() * 1000 + 200));
        
        const executionTime = performance.now() - startTime;
        
        // Parse query type
        const queryType = this.detectQueryType(sql);
        
        // Generate mock results based on query type
        let results;
        if (queryType === 'COUNT') {
            results = this.generateCountResults();
        } else if (queryType === 'AGGREGATION') {
            results = this.generateAggregationResults();
        } else {
            results = this.generateSelectResults(maxRows);
        }
        
        return {
            columns: results.columns,
            rows: results.rows,
            rowCount: results.rows.length,
            executionTime,
            bytesScanned: this.estimateBytesScanned(results.rows.length)
        };
    }

    detectQueryType(sql) {
        const upperSql = sql.toUpperCase();
        
        if (upperSql.includes('COUNT(')) {
            return 'COUNT';
        } else if (upperSql.includes('GROUP BY') || upperSql.includes('AVG(') || 
                   upperSql.includes('SUM(') || upperSql.includes('MAX(') || upperSql.includes('MIN(')) {
            return 'AGGREGATION';
        } else {
            return 'SELECT';
        }
    }

    generateCountResults() {
        return {
            columns: [
                { name: "count", type: "bigint", nullable: false }
            ],
            rows: [
                { count: Math.floor(Math.random() * 1000000) + 1000 }
            ]
        };
    }

    generateAggregationResults() {
        const groupCount = Math.floor(Math.random() * 20) + 5;
        const rows = [];
        
        for (let i = 0; i < groupCount; i++) {
            rows.push({
                category: `Category ${String.fromCharCode(65 + i)}`,
                count: Math.floor(Math.random() * 10000) + 100,
                avg_value: Math.round(Math.random() * 1000 * 100) / 100,
                sum_value: Math.floor(Math.random() * 100000) + 1000
            });
        }
        
        return {
            columns: [
                { name: "category", type: "varchar", nullable: false },
                { name: "count", type: "bigint", nullable: false },
                { name: "avg_value", type: "double", nullable: true },
                { name: "sum_value", type: "bigint", nullable: false }
            ],
            rows
        };
    }

    generateSelectResults(maxRows) {
        const rowCount = Math.min(maxRows, Math.floor(Math.random() * 200) + 50);
        const rows = [];
        
        for (let i = 0; i < rowCount; i++) {
            const row = {
                id: i + 1,
                name: `Record ${i + 1}`,
                value: Math.round(Math.random() * 1000 * 100) / 100,
                category: ['A', 'B', 'C', 'D'][Math.floor(Math.random() * 4)],
                created_at: new Date(Date.now() - Math.random() * 365 * 24 * 60 * 60 * 1000).toISOString(),
                active: Math.random() > 0.3
            };
            
            // Add some null values randomly
            if (Math.random() < 0.1) row.name = null;
            if (Math.random() < 0.05) row.value = null;
            if (Math.random() < 0.03) row.category = null;
            
            rows.push(row);
        }
        
        return {
            columns: [
                { name: "id", type: "bigint", nullable: false },
                { name: "name", type: "varchar", nullable: true },
                { name: "value", type: "double", nullable: true },
                { name: "category", type: "varchar", nullable: true },
                { name: "created_at", type: "timestamp", nullable: true },
                { name: "active", type: "boolean", nullable: false }
            ],
            rows
        };
    }

    estimateBytesScanned(rowCount) {
        // Rough estimate: average 100 bytes per row
        return rowCount * 100;
    }

    async explainQuery(sql, format = 'json') {
        await this.initialize();
        
        console.log(`Explaining query: ${sql}`);
        
        // Mock query plan
        const plan = {
            "Query Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "data",
                "Alias": "data",
                "Startup Cost": 0.00,
                "Total Cost": 1000.00,
                "Plan Rows": 1000,
                "Plan Width": 100
            }
        };
        
        return format === 'json' ? JSON.stringify(plan, null, 2) : plan.toString();
    }

    async parseQuery(sql) {
        await this.initialize();
        
        try {
            // Basic SQL validation
            const trimmed = sql.trim();
            if (!trimmed) {
                throw new Error("Empty query");
            }
            
            // Check for basic SQL keywords
            const upperSql = trimmed.toUpperCase();
            const validStarts = ['SELECT', 'WITH', 'EXPLAIN'];
            const hasValidStart = validStarts.some(keyword => upperSql.startsWith(keyword));
            
            if (!hasValidStart) {
                throw new Error("Query must start with SELECT, WITH, or EXPLAIN");
            }
            
            // Check for balanced parentheses
            let parenCount = 0;
            for (const char of sql) {
                if (char === '(') parenCount++;
                if (char === ')') parenCount--;
                if (parenCount < 0) throw new Error("Unmatched closing parenthesis");
            }
            if (parenCount > 0) throw new Error("Unmatched opening parenthesis");
            
            return {
                valid: true,
                type: this.detectQueryType(sql)
            };
        } catch (error) {
            return {
                valid: false,
                error: error.message
            };
        }
    }

    async analyzeTable(tableName) {
        await this.initialize();
        
        console.log(`Analyzing table: ${tableName}`);
        
        // Mock table analysis
        return {
            rowCount: Math.floor(Math.random() * 1000000) + 1000,
            columns: [
                {
                    name: "id",
                    type: "bigint",
                    nullable: false,
                    distinctCount: Math.floor(Math.random() * 100000) + 1000,
                    nullCount: 0,
                    minValue: 1,
                    maxValue: 1000000
                },
                {
                    name: "name",
                    type: "varchar",
                    nullable: true,
                    distinctCount: Math.floor(Math.random() * 50000) + 500,
                    nullCount: Math.floor(Math.random() * 1000),
                    minValue: "AAAA",
                    maxValue: "ZZZZ"
                },
                {
                    name: "value",
                    type: "double",
                    nullable: true,
                    distinctCount: Math.floor(Math.random() * 10000) + 100,
                    nullCount: Math.floor(Math.random() * 500),
                    minValue: 0.01,
                    maxValue: 999.99
                }
            ]
        };
    }
}

// Global instance
window.duckdbWrapper = new DuckDBWrapper();

// Export functions for C# interop
window.initializeDuckDB = async () => {
    return await window.duckdbWrapper.initialize();
};

window.registerParquetFile = async (fileId, bytes) => {
    return await window.duckdbWrapper.registerParquetFile(fileId, bytes);
};

window.executeQuery = async (sql, maxRows) => {
    return await window.duckdbWrapper.executeQuery(sql, maxRows);
};

window.explainQuery = async (sql, format) => {
    return await window.duckdbWrapper.explainQuery(sql, format);
};

window.parseQuery = async (sql) => {
    return await window.duckdbWrapper.parseQuery(sql);
};

window.analyzeTable = async (tableName) => {
    return await window.duckdbWrapper.analyzeTable(tableName);
};

console.log('DuckDB wrapper module loaded');