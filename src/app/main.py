from .manager import DataManager


def main():
    manager = DataManager()
    
    manager.setup_weapons_detector()
    
    processed_count = manager.run_full_pipeline()
    
    print(f"Processed {processed_count} tweets successfully")

if __name__ == "__main__":
    main()
