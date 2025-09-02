# Week_12_ElasticSearch_Malicious_Text
a tweets analasis system with elastic-search

## API Endpoints

The FastAPI server provides the following endpoints:

- **GET /** - API information and available endpoints
- **GET /tweets** - Returns all tweets from Elasticsearch
- **GET /tweets/multiple-weapons** - Returns tweets with 2 or more weapons

## Running the Server

1. Make sure Elasticsearch is running
2. Run the FastAPI server:
   ```bash
   python -m src.app.main
   ```
   
   Or with uvicorn directly:
   ```bash
   uvicorn src.app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

3. Open http://localhost:8000 in your browser
4. Visit http://localhost:8000/docs for interactive API documentation

## Features

- **Lifespan Management**: The application runs the full data processing pipeline on startup
- **Automatic Data Processing**: Processes tweets, applies sentiment analysis, and detects weapons
- **REST API**: Provides easy access to processed data via HTTP endpoints
- **Real-time Documentation**: FastAPI automatically generates OpenAPI documentation
