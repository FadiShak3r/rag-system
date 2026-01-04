# Quick Start Guide - Step by Step

## Prerequisites

1. **Python 3.8+** installed
2. **ODBC Driver 18 for SQL Server** installed on your system
3. **OpenAI API Key** (get one at https://platform.openai.com/api-keys)

---

## Step 1: Install Dependencies

Open terminal in the project directory and run:

```bash
pip3 install -r requirements.txt
```

Or if you prefer pip:

```bash
pip install -r requirements.txt
```

---

## Step 2: Create .env File

Create a `.env` file in the project root directory with the following content:

```env
# SQL Server Configuration
MSSQL_DRIVER=ODBC Driver 18 for SQL Server
MSSQL_SERVER=134.103.216.87
MSSQL_PORT=1433
MSSQL_DATABASE=AdventureWorksDW2022
MSSQL_UID=SA
MSSQL_PWD=MeinSicheresPW123!
MSSQL_TRUST_SERVER_CERTIFICATE=yes

# OpenAI API Configuration
OPENAI_API_KEY=your_openai_api_key_here

# Optional: ChromaDB Path (defaults to ./chroma_db if not set)
# CHROMA_DB_PATH=./chroma_db
```

**Important:**
- Replace `your_openai_api_key_here` with your actual OpenAI API key
- Make sure there are **no spaces** around the `=` sign
- Save the file as **UTF-8 without BOM** encoding

---

## Step 3: Test Database Connection (Optional but Recommended)

Test your SQL Server connection:

```bash
python3 test_db_connection.py
```

This will verify:
- ✅ Configuration is correct
- ✅ Database connection works
- ✅ Table access is working
- ✅ Row counts

If this fails, fix the connection issues before proceeding.

---

## Step 4: Index the Database

This step extracts data from your SQL Server table (`DimProduct`), creates embeddings, and stores them in the vector database.

**First time indexing:**
```bash
python3 indexer.py --clear
```

**Subsequent indexing (updates existing data):**
```bash
python3 indexer.py
```

**What happens:**
1. Connects to SQL Server
2. Extracts data from `DimProduct` table
3. Processes and chunks the data
4. Generates OpenAI embeddings (this may take a while depending on data size)
5. Stores everything in ChromaDB vector store

**Expected output:**
```
Starting database indexing...

1. Connecting to database and extracting data...
Connected to SQL Server database: AdventureWorksDW2022
Retrieved X rows from DimProduct

2. Processing and chunking data...
Created Y chunks from Z documents

3. Generating embeddings...
   Processing Y chunks with batch size 10 and 1s delay between batches
   ✓ Generated embeddings for batch 1/N (10 texts)
   ...

4. Storing in vector database...
Added Y documents to vector store

✓ Indexing complete! Indexed Y chunks.
  Vector store contains X documents.
```

**Note:** The embedding generation step can take several minutes depending on:
- Amount of data in your table
- Your OpenAI API rate limits
- Network speed

---

## Step 5: Start the Chatbot

Once indexing is complete, start the web chatbot:

```bash
python3 app.py
```

**Expected output:**
```
Initializing RAG system...
Creating RAGSystem instance...
  - Initializing OpenAI client...
  - Creating EmbeddingGenerator...
  - Creating VectorStore...
Loaded existing collection: products_data
  - RAGSystem initialization complete
RAGSystem created, getting stats...
✓ RAG system ready! (X documents indexed)
Starting Flask server on http://0.0.0.0:4100
 * Running on http://0.0.0.0:4100
 * Debug mode: on
```

---

## Step 6: Open the Chatbot in Browser

Open your web browser and navigate to:

```
http://localhost:4100
```

You should see the chatbot interface where you can ask questions about your product data!

**Example questions:**
- "What products are available?"
- "Show me products with the highest list price"
- "What colors are available?"
- "Tell me about products in the M category"

---

## Troubleshooting

### Issue: "Python-dotenv could not parse statement"
**Solution:** Check your `.env` file:
- No spaces around `=` sign
- Save as UTF-8 without BOM
- Each variable on its own line

### Issue: "Error connecting to SQL Server"
**Solution:**
- Verify ODBC Driver 18 is installed
- Check SQL Server credentials in `.env`
- Test connection with `python3 test_db_connection.py`
- Verify SQL Server is accessible from your network

### Issue: "OPENAI_API_KEY not found"
**Solution:**
- Make sure `.env` file exists in project root
- Check `OPENAI_API_KEY` is set correctly
- Verify no extra spaces or quotes around the key

### Issue: App hangs at "getting stats..."
**Solution:** This is now fixed! The app will timeout after 3 seconds and continue.

### Issue: "No data found in tables"
**Solution:**
- Verify table name in `config.py` is correct: `DimProduct`
- Check database connection works
- Verify table has data

### Issue: OpenAI API errors
**Solution:**
- Check API key is valid
- Verify billing/quota at https://platform.openai.com/account/billing
- Wait a few minutes if rate limited

---

## Daily Updates (Optional)

To keep your data synchronized, you can run daily updates:

**Run once:**
```bash
python3 daily_sync.py --run-once
```

**Run as scheduled task (runs daily at 2:00 AM):**
```bash
python3 daily_sync.py
```

---

## Project Structure

```
├── config.py              # Configuration (reads .env)
├── database.py            # SQL Server connection
├── data_processor.py      # Data processing and chunking
├── embedding.py           # OpenAI embedding generation
├── vector_store.py        # ChromaDB vector storage
├── rag_system.py          # RAG query system
├── indexer.py             # Main indexing script
├── app.py                 # Flask web application
├── templates/
│   └── chatbot.html       # Web interface
├── chroma_db/             # Vector database (created automatically)
├── .env                   # Your configuration (create this)
└── requirements.txt       # Python dependencies
```

---

## Next Steps

1. ✅ Index your database: `python3 indexer.py --clear`
2. ✅ Start the chatbot: `python3 app.py`
3. ✅ Open browser: `http://localhost:4100`
4. ✅ Start asking questions!

Enjoy your RAG-powered chatbot! 🚀

