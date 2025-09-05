const path = require('path');
const webpack = require('webpack');

module.exports = {
    entry: {
        'parquet-processor': './wwwroot/js/parquet-processor.js',
        'duckdb-wrapper': './wwwroot/js/duckdb-wrapper.js',
        'storage-service': './wwwroot/js/storage-service.js',
        'ui-helpers': './wwwroot/js/ui-helpers.js'
    },
    output: {
        path: path.resolve(__dirname, 'wwwroot/dist'),
        filename: '[name].bundle.js',
        library: {
            type: 'window'
        },
        clean: true
    },
    mode: process.env.NODE_ENV === 'production' ? 'production' : 'development',
    devtool: process.env.NODE_ENV === 'production' ? 'source-map' : 'eval-source-map',
    module: {
        rules: [
            {
                test: /\.js$/,
                exclude: /node_modules/,
                use: {
                    loader: 'babel-loader',
                    options: {
                        presets: ['@babel/preset-env']
                    }
                }
            },
            {
                test: /\.css$/,
                use: ['style-loader', 'css-loader']
            },
            {
                test: /\.wasm$/,
                type: 'asset/resource'
            }
        ]
    },
    resolve: {
        extensions: ['.js', '.wasm'],
        fallback: {
            "path": false,
            "fs": false,
            "crypto": false,
            "worker_threads": false
        }
    },
    plugins: [
        new webpack.DefinePlugin({
            'process.env.NODE_ENV': JSON.stringify(process.env.NODE_ENV || 'development')
        })
    ],
    optimization: {
        splitChunks: {
            chunks: 'all',
            cacheGroups: {
                vendor: {
                    test: /[\\/]node_modules[\\/]/,
                    name: 'vendors',
                    chunks: 'all',
                },
                arrow: {
                    test: /[\\/]node_modules[\\/]apache-arrow/,
                    name: 'arrow',
                    chunks: 'all',
                },
                duckdb: {
                    test: /[\\/]node_modules[\\/]@duckdb/,
                    name: 'duckdb',
                    chunks: 'all',
                }
            }
        }
    },
    experiments: {
        asyncWebAssembly: true,
        topLevelAwait: true
    }
};