# Parquet & Delta Lake Tool

A comprehensive web-based toolkit for working with Parquet files and Delta Lake tables, built with Blazor WebAssembly.

![Build Status](https://github.com/yourusername/parquet/actions/workflows/deploy.yml/badge.svg)

## 🚀 Features

### Core Features
- **File Upload & Processing**: Support for Parquet, CSV, and JSON files
- **Data Preview**: Interactive data grid with virtual scrolling for large datasets
- **Schema Exploration**: Visual schema browser with nested type support
- **Query Engine**: SQL query interface for data analysis
- **Statistics Dashboard**: Column statistics and data quality metrics

### Planned Features
- **Delta Lake Support**: Version history, time travel, OPTIMIZE, and VACUUM operations
- **Format Conversion**: Convert between Parquet, CSV, JSON, and other formats
- **Data Quality Analysis**: Automated data profiling and anomaly detection
- **Cloud Integration**: Support for S3, Azure Blob Storage, and Google Cloud Storage
- **Performance Analysis**: Query optimization recommendations

## 🛠️ Technology Stack

- **Frontend**: Blazor WebAssembly (.NET 9)
- **UI Framework**: MudBlazor
- **Data Processing**: Apache Arrow, DuckDB WASM (planned)
- **Storage**: IndexedDB, Origin Private File System (OPFS)
- **Deployment**: GitHub Pages

## 🏃‍♂️ Getting Started

### Prerequisites
- .NET 9 SDK
- Node.js 20+ (for npm packages)

### Local Development
```bash
# Clone the repository
git clone https://github.com/yourusername/parquet.git
cd parquet

# Restore dependencies
dotnet restore ParquetDeltaTool/

# Install npm packages (if any)
cd ParquetDeltaTool && npm install

# Run the application
dotnet run --project ParquetDeltaTool/
```

The application will be available at `https://localhost:5001`.

### Building for Production
```bash
# Publish the application
dotnet publish ParquetDeltaTool/ -c Release -o dist/

# The published files will be in the dist/wwwroot directory
```

## 📁 Project Structure

```
ParquetDeltaTool/
├── Components/          # Reusable Blazor components
├── Models/             # Data models and DTOs
├── Services/           # Business logic and service interfaces
├── State/              # Application state management
├── Pages/              # Blazor pages/routes
├── Layout/             # Layout components
└── wwwroot/            # Static assets
```

## 🔧 Architecture

The application follows a clean architecture pattern:

- **Presentation Layer**: Blazor components and pages
- **Business Logic**: Services implementing core functionality
- **Data Access**: Storage services for IndexedDB and OPFS
- **Models**: Domain models and data transfer objects

## 🚀 Deployment

The application is automatically deployed to GitHub Pages using GitHub Actions. Every push to the `main` branch triggers a build and deployment.

**Live Demo**: [https://yourusername.github.io/parquet/](https://yourusername.github.io/parquet/)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development Guidelines
1. Follow the existing code style and patterns
2. Add tests for new features
3. Update documentation as needed
4. Ensure all builds pass before submitting PR

## 📋 Roadmap

### Phase 1: Core Infrastructure ✅
- [x] Blazor WebAssembly setup
- [x] MudBlazor UI framework
- [x] Basic file upload
- [x] Data preview functionality
- [x] GitHub Pages deployment

### Phase 2: Data Processing (In Progress)
- [ ] Parquet WASM integration
- [ ] DuckDB WASM for SQL queries
- [ ] Advanced schema visualization
- [ ] Statistics computation

### Phase 3: Delta Lake Support
- [ ] Delta transaction log parsing
- [ ] Time travel interface
- [ ] Version comparison
- [ ] OPTIMIZE and VACUUM operations

### Phase 4: Advanced Features
- [ ] Cloud storage connectors
- [ ] Format conversion tools
- [ ] Data quality analysis
- [ ] Performance optimization tools

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Apache Parquet](https://parquet.apache.org/) for the columnar storage format
- [Delta Lake](https://delta.io/) for the lakehouse architecture
- [MudBlazor](https://mudblazor.com/) for the component library
- [DuckDB](https://duckdb.org/) for the query engine
- [Apache Arrow](https://arrow.apache.org/) for columnar data processing

## 📞 Support

If you have questions or need help, please:
1. Check the [Issues](https://github.com/yourusername/parquet/issues) page
2. Create a new issue if needed
3. Join our [Discussions](https://github.com/yourusername/parquet/discussions)

---

**Note**: This is a client-side application that processes data entirely in your browser. No data is sent to external servers, ensuring privacy and security.