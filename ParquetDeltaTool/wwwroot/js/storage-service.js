// IndexedDB Storage Service
// Real implementation for browser-based data storage

class StorageService {
    constructor() {
        this.dbName = 'ParquetDeltaToolDB';
        this.dbVersion = 1;
        this.db = null;
        this.initialized = false;
    }

    async initialize() {
        if (this.initialized) return true;

        return new Promise((resolve, reject) => {
            const request = indexedDB.open(this.dbName, this.dbVersion);

            request.onerror = () => {
                console.error('Failed to open IndexedDB:', request.error);
                reject(request.error);
            };

            request.onsuccess = () => {
                this.db = request.result;
                this.initialized = true;
                console.log('IndexedDB initialized successfully');
                resolve(true);
            };

            request.onupgradeneeded = (event) => {
                const db = event.target.result;
                
                // File metadata store
                if (!db.objectStoreNames.contains('fileMetadata')) {
                    const metadataStore = db.createObjectStore('fileMetadata', { keyPath: 'fileId' });
                    metadataStore.createIndex('fileName', 'fileName', { unique: false });
                    metadataStore.createIndex('format', 'format', { unique: false });
                    metadataStore.createIndex('modifiedAt', 'modifiedAt', { unique: false });
                    metadataStore.createIndex('uploadedAt', 'uploadedAt', { unique: false });
                }

                // File content store (for smaller files)
                if (!db.objectStoreNames.contains('fileContent')) {
                    db.createObjectStore('fileContent', { keyPath: 'fileId' });
                }

                // Large file chunks store (for big files)
                if (!db.objectStoreNames.contains('fileChunks')) {
                    const chunksStore = db.createObjectStore('fileChunks', { keyPath: ['fileId', 'chunkIndex'] });
                    chunksStore.createIndex('fileId', 'fileId', { unique: false });
                }

                // Query history store
                if (!db.objectStoreNames.contains('queryHistory')) {
                    const queryStore = db.createObjectStore('queryHistory', { keyPath: 'id', autoIncrement: true });
                    queryStore.createIndex('executedAt', 'executedAt', { unique: false });
                    queryStore.createIndex('fileId', 'fileId', { unique: false });
                }

                // Schema cache store
                if (!db.objectStoreNames.contains('schemaCache')) {
                    db.createObjectStore('schemaCache', { keyPath: 'fileId' });
                }

                // Settings store
                if (!db.objectStoreNames.contains('settings')) {
                    db.createObjectStore('settings', { keyPath: 'key' });
                }

                console.log('IndexedDB schema created/upgraded');
            };
        });
    }

    async storeFile(fileId, fileName, content, metadata = {}) {
        await this.initialize();

        const transaction = this.db.transaction(['fileMetadata', 'fileContent', 'fileChunks'], 'readwrite');
        
        try {
            // Store metadata
            const metadataStore = transaction.objectStore('fileMetadata');
            const fileMetadata = {
                fileId,
                fileName,
                fileSize: content.byteLength || content.length,
                format: this.detectFileFormat(fileName),
                uploadedAt: new Date().toISOString(),
                modifiedAt: new Date().toISOString(),
                ...metadata
            };
            
            await this.putData(metadataStore, fileMetadata);

            // Store content - use chunking for large files (>10MB)
            const MAX_CHUNK_SIZE = 10 * 1024 * 1024; // 10MB
            const contentSize = content.byteLength || content.length;

            if (contentSize <= MAX_CHUNK_SIZE) {
                // Store as single blob for smaller files
                const contentStore = transaction.objectStore('fileContent');
                await this.putData(contentStore, { fileId, content });
            } else {
                // Store in chunks for large files
                const chunksStore = transaction.objectStore('fileChunks');
                const numChunks = Math.ceil(contentSize / MAX_CHUNK_SIZE);

                for (let i = 0; i < numChunks; i++) {
                    const start = i * MAX_CHUNK_SIZE;
                    const end = Math.min(start + MAX_CHUNK_SIZE, contentSize);
                    const chunk = content.slice(start, end);
                    
                    await this.putData(chunksStore, {
                        fileId,
                        chunkIndex: i,
                        chunkData: chunk,
                        totalChunks: numChunks
                    });
                }
            }

            await this.waitForTransaction(transaction);
            console.log(`File ${fileName} stored successfully`);
            return fileMetadata;

        } catch (error) {
            console.error('Error storing file:', error);
            throw error;
        }
    }

    async getFile(fileId) {
        await this.initialize();

        const transaction = this.db.transaction(['fileContent', 'fileChunks'], 'readonly');
        
        try {
            // Try to get as single content first
            const contentStore = transaction.objectStore('fileContent');
            const contentResult = await this.getData(contentStore, fileId);
            
            if (contentResult) {
                return contentResult.content;
            }

            // If not found, try to reconstruct from chunks
            const chunksStore = transaction.objectStore('fileChunks');
            const chunksIndex = chunksStore.index('fileId');
            const chunks = await this.getAllFromIndex(chunksIndex, fileId);

            if (chunks.length === 0) {
                throw new Error(`File content not found for ID: ${fileId}`);
            }

            // Sort chunks by index and reconstruct
            chunks.sort((a, b) => a.chunkIndex - b.chunkIndex);
            
            // Calculate total size
            let totalSize = 0;
            chunks.forEach(chunk => {
                totalSize += chunk.chunkData.byteLength || chunk.chunkData.length;
            });

            // Reconstruct file
            const reconstructed = new Uint8Array(totalSize);
            let offset = 0;
            
            chunks.forEach(chunk => {
                const chunkArray = new Uint8Array(chunk.chunkData);
                reconstructed.set(chunkArray, offset);
                offset += chunkArray.length;
            });

            return reconstructed.buffer;

        } catch (error) {
            console.error('Error retrieving file:', error);
            throw error;
        }
    }

    async getFileMetadata(fileId) {
        await this.initialize();

        const transaction = this.db.transaction(['fileMetadata'], 'readonly');
        const store = transaction.objectStore('fileMetadata');
        
        return await this.getData(store, fileId);
    }

    async getRecentFiles(limit = 50) {
        await this.initialize();

        const transaction = this.db.transaction(['fileMetadata'], 'readonly');
        const store = transaction.objectStore('fileMetadata');
        const index = store.index('uploadedAt');
        
        return new Promise((resolve, reject) => {
            const request = index.openCursor(null, 'prev'); // Descending order
            const results = [];
            let count = 0;

            request.onsuccess = () => {
                const cursor = request.result;
                if (cursor && count < limit) {
                    results.push(cursor.value);
                    count++;
                    cursor.continue();
                } else {
                    resolve(results);
                }
            };

            request.onerror = () => reject(request.error);
        });
    }

    async deleteFile(fileId) {
        await this.initialize();

        const transaction = this.db.transaction(['fileMetadata', 'fileContent', 'fileChunks', 'schemaCache'], 'readwrite');
        
        try {
            // Delete metadata
            const metadataStore = transaction.objectStore('fileMetadata');
            await this.deleteData(metadataStore, fileId);

            // Delete content
            const contentStore = transaction.objectStore('fileContent');
            await this.deleteData(contentStore, fileId);

            // Delete chunks
            const chunksStore = transaction.objectStore('fileChunks');
            const chunksIndex = chunksStore.index('fileId');
            const chunks = await this.getAllFromIndex(chunksIndex, fileId);
            
            for (const chunk of chunks) {
                await this.deleteData(chunksStore, [fileId, chunk.chunkIndex]);
            }

            // Delete cached schema
            const schemaStore = transaction.objectStore('schemaCache');
            await this.deleteData(schemaStore, fileId);

            await this.waitForTransaction(transaction);
            console.log(`File ${fileId} deleted successfully`);

        } catch (error) {
            console.error('Error deleting file:', error);
            throw error;
        }
    }

    async storeQueryHistory(query) {
        await this.initialize();

        const transaction = this.db.transaction(['queryHistory'], 'readwrite');
        const store = transaction.objectStore('queryHistory');
        
        const queryRecord = {
            ...query,
            executedAt: new Date().toISOString()
        };

        return await this.putData(store, queryRecord);
    }

    async getQueryHistory(limit = 100, fileId = null) {
        await this.initialize();

        const transaction = this.db.transaction(['queryHistory'], 'readonly');
        const store = transaction.objectStore('queryHistory');
        const index = store.index('executedAt');
        
        return new Promise((resolve, reject) => {
            const request = index.openCursor(null, 'prev');
            const results = [];
            let count = 0;

            request.onsuccess = () => {
                const cursor = request.result;
                if (cursor && count < limit) {
                    const record = cursor.value;
                    if (!fileId || record.fileId === fileId) {
                        results.push(record);
                        count++;
                    }
                    cursor.continue();
                } else {
                    resolve(results);
                }
            };

            request.onerror = () => reject(request.error);
        });
    }

    async storeSchema(fileId, schema) {
        await this.initialize();

        const transaction = this.db.transaction(['schemaCache'], 'readwrite');
        const store = transaction.objectStore('schemaCache');
        
        return await this.putData(store, {
            fileId,
            schema,
            cachedAt: new Date().toISOString()
        });
    }

    async getSchema(fileId) {
        await this.initialize();

        const transaction = this.db.transaction(['schemaCache'], 'readonly');
        const store = transaction.objectStore('schemaCache');
        
        const result = await this.getData(store, fileId);
        return result ? result.schema : null;
    }

    async getStorageUsage() {
        if ('storage' in navigator && 'estimate' in navigator.storage) {
            try {
                const estimate = await navigator.storage.estimate();
                return {
                    used: estimate.usage || 0,
                    quota: estimate.quota || 0,
                    percentage: estimate.quota ? (estimate.usage / estimate.quota) * 100 : 0
                };
            } catch (error) {
                console.warn('Unable to get storage estimate:', error);
            }
        }

        // Fallback estimation
        return {
            used: 0,
            quota: 50 * 1024 * 1024 * 1024, // 50GB default estimate
            percentage: 0
        };
    }

    async clearStorage() {
        await this.initialize();

        const storeNames = ['fileMetadata', 'fileContent', 'fileChunks', 'queryHistory', 'schemaCache'];
        const transaction = this.db.transaction(storeNames, 'readwrite');

        try {
            for (const storeName of storeNames) {
                const store = transaction.objectStore(storeName);
                await this.clearStore(store);
            }

            await this.waitForTransaction(transaction);
            console.log('Storage cleared successfully');

        } catch (error) {
            console.error('Error clearing storage:', error);
            throw error;
        }
    }

    // Helper methods
    detectFileFormat(fileName) {
        const extension = fileName.split('.').pop().toLowerCase();
        
        switch (extension) {
            case 'parquet': return 'Parquet';
            case 'csv': return 'CSV';
            case 'json': return 'JSON';
            case 'jsonl': case 'ndjson': return 'JSONL';
            case 'avro': return 'Avro';
            case 'orc': return 'ORC';
            default: return 'Unknown';
        }
    }

    // IndexedDB helper methods
    putData(store, data) {
        return new Promise((resolve, reject) => {
            const request = store.put(data);
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    getData(store, key) {
        return new Promise((resolve, reject) => {
            const request = store.get(key);
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    deleteData(store, key) {
        return new Promise((resolve, reject) => {
            const request = store.delete(key);
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    getAllFromIndex(index, key) {
        return new Promise((resolve, reject) => {
            const request = index.getAll(key);
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    clearStore(store) {
        return new Promise((resolve, reject) => {
            const request = store.clear();
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    waitForTransaction(transaction) {
        return new Promise((resolve, reject) => {
            transaction.oncomplete = () => resolve();
            transaction.onerror = () => reject(transaction.error);
            transaction.onabort = () => reject(new Error('Transaction aborted'));
        });
    }
}

// Global instance
window.storageService = new StorageService();

// Export functions for C# interop
window.initializeStorage = async () => {
    return await window.storageService.initialize();
};

window.storeFile = async (fileId, fileName, content, metadata) => {
    return await window.storageService.storeFile(fileId, fileName, content, metadata);
};

window.getFile = async (fileId) => {
    return await window.storageService.getFile(fileId);
};

window.getFileMetadata = async (fileId) => {
    return await window.storageService.getFileMetadata(fileId);
};

window.getRecentFiles = async (limit) => {
    return await window.storageService.getRecentFiles(limit);
};

window.deleteFile = async (fileId) => {
    return await window.storageService.deleteFile(fileId);
};

window.getStorageUsage = async () => {
    return await window.storageService.getStorageUsage();
};

window.storeQueryHistory = async (query) => {
    return await window.storageService.storeQueryHistory(query);
};

window.getQueryHistory = async (limit, fileId) => {
    return await window.storageService.getQueryHistory(limit, fileId);
};

window.storeSchema = async (fileId, schema) => {
    return await window.storageService.storeSchema(fileId, schema);
};

window.getSchema = async (fileId) => {
    return await window.storageService.getSchema(fileId);
};

console.log('Storage service module loaded');