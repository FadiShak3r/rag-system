"""
Daily sync script to update the RAG system with latest database data
"""
import schedule
import time
from indexer import index_database


def sync_data():
    """Sync data from database and re-index"""
    print(f"\n{'='*50}")
    print(f"Daily sync started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    
    try:
        index_database(clear_existing=True)
        print(f"Daily sync completed successfully at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        print(f"Error during daily sync: {e}")


def run_daily_sync():
    """Run the daily sync scheduler"""
    schedule.every().day.at("02:00").do(sync_data)
    
    print("Daily sync scheduler started. Will sync at 2:00 AM daily.")
    print("Press Ctrl+C to stop.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nScheduler stopped.")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--run-once":
        sync_data()
    else:
        run_daily_sync()

