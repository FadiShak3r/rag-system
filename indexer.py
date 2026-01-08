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
    
    # Process the data into documents
    print("\n2. Processing data into documents...")
    processor = DataProcessor()
    documents = processor.process_tables(tables_data)
    
    if not documents:
        print("No documents created. Exiting.")
        return
    
    # Generate embeddings
    print("\n3. Generating embeddings...")
    print(f"   Processing {len(documents)} documents")
    print("   (Using optimized batch size 100 with concurrent processing)")
    # Optimized settings: batch_size=100 (up to 2048 supported), concurrent processing with 3 workers
    embedding_gen = EmbeddingGenerator(batch_size=100, delay_between_batches=0.0, 
                                      max_workers=3, use_concurrent=True)
    
    try:
        documents_with_embeddings = embedding_gen.add_embeddings_to_chunks(documents)
    except Exception as e:
        print(f"\n❌ Error during embedding generation: {e}")
        print("\nTips:")
        print("  - Check your OpenAI API quota at https://platform.openai.com/account/billing")
        print("  - Wait a few minutes and try again (rate limits reset over time)")
        raise
    
    # Store in vector database
    print("\n4. Storing in vector database...")
    print("   Creating vector store connection...")
    vector_store = VectorStore()
    
    if clear_existing:
        print("   Clearing existing collection...")
        vector_store.clear_collection()
    
    print("   Adding documents to vector store...")
    vector_store.add_documents(documents_with_embeddings)
    
    print(f"\n✓ Indexing complete! Indexed {len(documents_with_embeddings)} documents.")
    
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
    import os
    import shutil
    
    clear = False
    reset_db = False
    
    if len(sys.argv) > 1:
        if "--clear" in sys.argv:
            clear = True
            print("Will clear existing collection before indexing.")
        if "--reset-db" in sys.argv:
            reset_db = True
            clear = True
            print("Will reset ChromaDB database (delete and recreate).")
    
    # If resetting, delete the chroma_db directory
    if reset_db:
        from config import CHROMA_DB_PATH
        if os.path.exists(CHROMA_DB_PATH):
            print(f"Deleting ChromaDB directory: {CHROMA_DB_PATH}")
            try:
                shutil.rmtree(CHROMA_DB_PATH)
                print("✓ ChromaDB directory deleted")
            except Exception as e:
                print(f"⚠ Error deleting ChromaDB directory: {e}")
                print("  Continuing anyway...")
    
    try:
        index_database(clear_existing=clear)
    except KeyboardInterrupt:
        print("\n\n⚠ Indexing interrupted by user")
        print("  Partial data may have been saved. Run again to continue.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error during indexing: {e}")
        import traceback
        traceback.print_exc()
        print("\n💡 Tips:")
        print("  - If ChromaDB seems corrupted, try: python3 indexer.py --reset-db --clear")
        print("  - Check your OpenAI API quota and billing")
        print("  - Verify database connection is working")
        sys.exit(1)

