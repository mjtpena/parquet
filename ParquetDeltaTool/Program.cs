using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using ParquetDeltaTool;
using ParquetDeltaTool.Services;
using ParquetDeltaTool.State;
using MudBlazor.Services;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

builder.Services.AddScoped(sp => new HttpClient { BaseAddress = new Uri(builder.HostEnvironment.BaseAddress) });

// Add MudBlazor services
builder.Services.AddMudServices();

// Add application services
builder.Services.AddSingleton<ApplicationState>();
builder.Services.AddScoped<IFileProcessor, FileProcessorService>();
builder.Services.AddScoped<IStorageService, StorageService>();
builder.Services.AddScoped<IQueryEngine, QueryEngineService>();

await builder.Build().RunAsync();
