using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using ParquetDeltaTool;
using ParquetDeltaTool.Services;
using ParquetDeltaTool.State;
using ParquetDeltaTool.Testing;
using MudBlazor.Services;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

builder.Services.AddScoped(sp => new HttpClient { BaseAddress = new Uri(builder.HostEnvironment.BaseAddress) });

// Add MudBlazor services
builder.Services.AddMudServices();

// Add application state
builder.Services.AddSingleton<ApplicationState>();

// Add core services
builder.Services.AddScoped<IFileProcessor, FileProcessorService>();
builder.Services.AddScoped<IStorageService, StorageService>();
builder.Services.AddScoped<IQueryEngine, QueryEngineService>();

// Add Delta Lake and schema services
builder.Services.AddScoped<IDeltaLakeService, DeltaLakeService>();
builder.Services.AddScoped<ISchemaManagementService, SchemaManagementService>();

// Add analytics and export services
builder.Services.AddScoped<IAdvancedAnalyticsService, AdvancedAnalyticsService>();
builder.Services.AddScoped<IDataExportService, DataExportService>();

// Add security and testing services
builder.Services.AddScoped<ISecurityService, SecurityService>();
builder.Services.AddScoped<ITestingService, TestingService>();

await builder.Build().RunAsync();
