from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from typing import List, Dict, Any
from .manager import DataManager

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting application and initializing data pipeline...")
    app.state.manager = DataManager()
    app.state.manager.setup_weapons_detector()
    processed_count = app.state.manager.run_full_pipeline()
    print(f"Processed {processed_count} tweets successfully during startup")
    yield
    print("Shutting down application...")

app = FastAPI(
    title="Malicious Text Detection API",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/", response_model=Dict[str, str])
async def root():
    return {
        "message": "Malicious Text Detection API",
        "version": "1.0.0",
        "endpoints": [
            "/tweets - Get all tweets from Elasticsearch",
            "/tweets/multiple-weapons - Get tweets with 2+ weapons"
        ]
    }

@app.get("/tweets", response_model=List[Dict[str, Any]])
async def get_all_tweets(request: Request):
    try:
        return request.app.state.manager.search_tweets({"match_all": {}})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving tweets: {str(e)}")

@app.get("/tweets/multiple-weapons", response_model=List[Dict[str, Any]])
async def get_tweets_with_multiple_weapons(request: Request):
    try:
        query = {
            "bool": {
                "filter": [
                    {
                        "script": {
                            "script": {
                                "source": "doc['weapons'].size() >= 2"
                            }
                        }
                    }
                ]
            }
        }
        return request.app.state.manager.search_tweets(query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving tweets with multiple weapons: {str(e)}")

def main():
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()
