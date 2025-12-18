"""
Main indexing script to process database tables and create embeddings
"""
from database import DatabaseConnector
from data_processor import DataProcessor
from embedding import EmbeddingGenerator
from vector_store import VectorStore
from config import TABLES


def index_database(clear_existing: bool = False):
    """Main function to index all database tables"""
    print("Starting database indexing...")
    
    # Connect to database and extract data
    print("\n1. Connecting to database and extracting data...")
    with DatabaseConnector() as db:
        tables_data = db.get_all_tables_data(TABLES)
    
    if not tables_data:
        print("No data found in tables. Exiting.")
        return
    
    # Process and chunk the data
    print("\n2. Processing and chunking data...")
    processor = DataProcessor()
    chunks = processor.process_tables(tables_data)
    
    if not chunks:
        print("No chunks created. Exiting.")
        return
    
    # Generate embeddings
    print("\n3. Generating embeddings...")
    print(f"   Processing {len(chunks)} chunks with batch size 10 and 1s delay between batches")
    print("   (This helps avoid rate limits. Adjust in embedding.py if needed)")
    embedding_gen = EmbeddingGenerator(batch_size=10, delay_between_batches=1.0)
    
    try:
        chunks_with_embeddings = embedding_gen.add_embeddings_to_chunks(chunks)
    except Exception as e:
        print(f"\n Error during embedding generation: {e}")
        print("\nTips:")
        print("  - Check your OpenAI API quota at https://platform.openai.com/account/billing")
        print("  - Wait a few minutes and try again (rate limits reset over time)")
        print("  - Reduce batch_size or increase delay_between_batches in embedding.py")
        raise
    
    # Store in vector database
    print("\n4. Storing in vector database...")
    vector_store = VectorStore()
    
    if clear_existing:
        vector_store.clear_collection()
    
    vector_store.add_documents(chunks_with_embeddings)
    
    print(f"\n✓ Indexing complete! Indexed {len(chunks_with_embeddings)} chunks.")
    
    # Try to get collection count (has built-in timeout, won't hang)
    print("  Getting collection count (this may take a moment for large collections)...")
    try:
        count = vector_store.get_collection_count()
        if count >= 0:
            print(f"  ✓ Vector store contains {count} documents.")
        else:
            print(f"  ✓ Vector store updated (exact count unavailable - collection is large)")
    except Exception as e:
        print(f"  ✓ Vector store updated (count unavailable: {e})")
    
    print("\n🎉 All done! You can now start the chatbot with: python3 app.py")


if __name__ == "__main__":
    import sys
    
    clear = False
    if len(sys.argv) > 1 and sys.argv[1] == "--clear":
        clear = True
        print("Will clear existing collection before indexing.")
    
    index_database(clear_existing=clear)

