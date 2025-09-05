// Keyboard shortcuts and UI interactions for Parquet Delta Tool

window.setupKeyboardShortcuts = (dotNetRef) => {
    // Remove any existing event listeners to prevent duplicates
    document.removeEventListener('keydown', window.keydownHandler);
    
    // Define the keyboard event handler
    window.keydownHandler = (event) => {
        const { key, ctrlKey, metaKey, shiftKey, altKey } = event;
        const isCtrlOrCmd = ctrlKey || metaKey;
        
        // Check for specific keyboard shortcuts
        let shortcut = '';
        
        if (isCtrlOrCmd && key === 'k') {
            event.preventDefault();
            shortcut = 'Ctrl+K';
        } else if (key === 'F5') {
            event.preventDefault();
            shortcut = 'F5';
        } else if (isCtrlOrCmd && shiftKey && key === 'T') {
            event.preventDefault();
            shortcut = 'Ctrl+Shift+T';
        } else if (isCtrlOrCmd && key === 'o') {
            event.preventDefault();
            shortcut = 'Ctrl+O';
        } else if (isCtrlOrCmd && key === 's') {
            event.preventDefault();
            shortcut = 'Ctrl+S';
        } else if (isCtrlOrCmd && key === 'f') {
            event.preventDefault();
            shortcut = 'Ctrl+F';
        } else if (altKey && key === 's') {
            event.preventDefault();
            shortcut = 'Alt+S';
        } else if (key === 'F1') {
            event.preventDefault();
            shortcut = 'F1';
        } else if (isCtrlOrCmd && key === '?') {
            event.preventDefault();
            shortcut = 'Ctrl+?';
        } else if (key === 'F11') {
            event.preventDefault();
            shortcut = 'F11';
        } else if (isCtrlOrCmd && key === '=') {
            event.preventDefault();
            shortcut = 'Ctrl++';
        } else if (isCtrlOrCmd && key === '-') {
            event.preventDefault();
            shortcut = 'Ctrl+-';
        }
        
        // Invoke the .NET method if a shortcut was detected
        if (shortcut && dotNetRef) {
            dotNetRef.invokeMethodAsync('HandleKeyboardShortcut', shortcut);
        }
    };
    
    // Add the event listener
    document.addEventListener('keydown', window.keydownHandler);
};

// Utility functions for UI interactions
window.uiHelpers = {
    // Focus management
    focusElement: (selector) => {
        const element = document.querySelector(selector);
        if (element) {
            element.focus();
        }
    },
    
    // Scroll utilities
    scrollToElement: (selector) => {
        const element = document.querySelector(selector);
        if (element) {
            element.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    },
    
    // Copy to clipboard
    copyToClipboard: async (text) => {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch (err) {
            console.error('Failed to copy text: ', err);
            return false;
        }
    },
    
    // Theme detection
    getPreferredTheme: () => {
        return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    },
    
    // Media queries
    isMobile: () => {
        return window.matchMedia('(max-width: 768px)').matches;
    },
    
    isTablet: () => {
        return window.matchMedia('(min-width: 769px) and (max-width: 1200px)').matches;
    },
    
    isDesktop: () => {
        return window.matchMedia('(min-width: 1201px)').matches;
    },
    
    // Performance monitoring
    measurePerformance: (name, fn) => {
        const start = performance.now();
        const result = fn();
        const end = performance.now();
        console.log(`${name} took ${end - start} milliseconds`);
        return result;
    },
    
    // Virtual scrolling helpers
    getVisibleRange: (containerHeight, itemHeight, scrollTop, buffer = 5) => {
        const startIndex = Math.max(0, Math.floor(scrollTop / itemHeight) - buffer);
        const endIndex = Math.min(
            Math.ceil((scrollTop + containerHeight) / itemHeight) + buffer
        );
        return { startIndex, endIndex };
    },
    
    // File size formatting
    formatFileSize: (bytes) => {
        if (bytes === 0) return '0 B';
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + ' ' + sizes[i];
    },
    
    // Number formatting
    formatNumber: (num) => {
        return new Intl.NumberFormat().format(num);
    },
    
    // Date formatting
    formatDate: (date, options = {}) => {
        return new Intl.DateTimeFormat('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            ...options
        }).format(new Date(date));
    },
    
    // Debounce utility
    debounce: (func, wait) => {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    },
    
    // Throttle utility
    throttle: (func, limit) => {
        let inThrottle;
        return function() {
            const args = arguments;
            const context = this;
            if (!inThrottle) {
                func.apply(context, args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    }
};

// Network status monitoring
window.networkMonitor = {
    isOnline: () => navigator.onLine,
    
    setupNetworkMonitoring: (dotNetRef) => {
        const updateNetworkStatus = () => {
            if (dotNetRef) {
                dotNetRef.invokeMethodAsync('UpdateNetworkStatus', navigator.onLine);
            }
        };
        
        window.addEventListener('online', updateNetworkStatus);
        window.addEventListener('offline', updateNetworkStatus);
        
        return () => {
            window.removeEventListener('online', updateNetworkStatus);
            window.removeEventListener('offline', updateNetworkStatus);
        };
    }
};

// Memory monitoring
window.memoryMonitor = {
    getMemoryInfo: () => {
        if ('memory' in performance) {
            return {
                used: performance.memory.usedJSHeapSize,
                total: performance.memory.totalJSHeapSize,
                limit: performance.memory.jsHeapSizeLimit
            };
        }
        return null;
    },
    
    formatMemorySize: (bytes) => {
        return window.uiHelpers.formatFileSize(bytes);
    }
};

// Performance monitoring
window.performanceMonitor = {
    startTiming: (name) => {
        performance.mark(`${name}-start`);
    },
    
    endTiming: (name) => {
        performance.mark(`${name}-end`);
        performance.measure(name, `${name}-start`, `${name}-end`);
        const measure = performance.getEntriesByName(name)[0];
        return measure.duration;
    },
    
    clearTimings: () => {
        performance.clearMarks();
        performance.clearMeasures();
    }
};

// Initialize on load
document.addEventListener('DOMContentLoaded', () => {
    console.log('Parquet Delta Tool UI utilities loaded');
});

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        setupKeyboardShortcuts: window.setupKeyboardShortcuts,
        uiHelpers: window.uiHelpers,
        networkMonitor: window.networkMonitor,
        memoryMonitor: window.memoryMonitor,
        performanceMonitor: window.performanceMonitor
    };
}