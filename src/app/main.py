from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from typing import List, Dict, Any
from .manager import DataManager

manager = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI application.
    Runs the full data processing pipeline on startup.
    """
    global manager
    
    print("Starting application and initializing data pipeline...")
    manager = DataManager()
    
    manager.setup_weapons_detector()
    
    processed_count = manager.run_full_pipeline()
    print(f"Processed {processed_count} tweets successfully during startup")
    
    yield
    
    print("Shutting down application...")

app = FastAPI(
    title="Malicious Text Detection API",
    description="API for detecting malicious text and weapons in tweets using Elasticsearch",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint returning API information"""
    return {
        "message": "Malicious Text Detection API",
        "version": "1.0.0",
        "endpoints": [
            "/tweets - Get all tweets from Elasticsearch",
            "/tweets/multiple-weapons - Get tweets with 2+ weapons"
        ]
    }

@app.get("/tweets", response_model=List[Dict[str, Any]])
async def get_all_tweets():
    """
    Get all tweets from Elasticsearch index
    """
    global manager
    
    if not manager:
        raise HTTPException(status_code=500, detail="Data manager not initialized")
    
    try:
        all_tweets = manager.search_tweets({"match_all": {}})
        return all_tweets
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving tweets: {str(e)}")

@app.get("/tweets/multiple-weapons", response_model=List[Dict[str, Any]])
async def get_tweets_with_multiple_weapons():
    """
    Get tweets that have 2 or more weapons in the 'weapons' field
    """
    global manager
    
    if not manager:
        raise HTTPException(status_code=500, detail="Data manager not initialized")
    
    try:
        all_tweets = manager.search_tweets({"match_all": {}})
        
        filtered_tweets = []
        for tweet in all_tweets:
            weapons = tweet.get('weapons', [])
            if isinstance(weapons, list) and len(weapons) >= 2:
                filtered_tweets.append(tweet)
            elif isinstance(weapons, str):
                weapon_list = [w.strip() for w in weapons.split(',') if w.strip()]
                if len(weapon_list) >= 2:
                    filtered_tweets.append(tweet)
        
        return filtered_tweets
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving tweets with multiple weapons: {str(e)}")

def main():
    """
    Main function for running the application with uvicorn
    """
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()
