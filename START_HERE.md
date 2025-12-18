# Quick Start Guide

## Step-by-Step Instructions

### 1. Install Dependencies

First, make sure you have Python 3.8+ installed, then install the required packages:

```bash
pip install -r requirements.txt
```

Or if you prefer pip3:

```bash
pip3 install -r requirements.txt
```

### 2. Configure Environment Variables

You already have a `.env` file. Make sure it contains:

```env
# MySQL Database Configuration
MYSQL_HOST=sql.freedb.tech
MYSQL_PORT=3306
MYSQL_USER=freedb_testauth
MYSQL_PASSWORD=3JVb4*42@g*SpXj
MYSQL_DATABASE=freedb_betterauth

# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key_here

# ChromaDB Configuration (optional)
CHROMA_DB_PATH=./chroma_db
```

**Important**: Replace `your_openai_api_key_here` with your actual OpenAI API key.

### 3. Check Your API Connection

Before indexing, verify your OpenAI API is working:

```bash
python check_api.py
```

This will test if your API key is valid and has quota.

### 4. Index Your Database

This step extracts data from your MySQL tables, creates embeddings, and stores them in the vector database:

```bash
python indexer.py
```

**First time?** Use `--clear` to start fresh:
```bash
python indexer.py --clear
```

This process may take a few minutes depending on:
- Amount of data in your tables
- Your OpenAI API rate limits
- Network speed

### 5. Start the Chatbot

Once indexing is complete, start the web chatbot:

```bash
python app.py
```

Then open your browser and navigate to `http://localhost:5000` to start chatting!

You can ask questions like:
- "What is the total revenue from all products?"
- "Which product has the highest sales?"
- "Show me products with the most purchases"
- "What are the top 5 products by revenue?"

## Troubleshooting

### If indexing fails:

1. **Check API status**: `python check_api.py`
2. **Verify database connection**: Make sure your MySQL credentials are correct
3. **Check quota**: If you see quota errors, check https://platform.openai.com/account/billing
4. **Rate limits**: The system auto-retries, but you may need to wait a few minutes

### If querying fails:

1. **Make sure indexing completed**: Run `python indexer.py` first
2. **Check vector store**: The `chroma_db` folder should exist with data

## Daily Updates

To keep your data fresh, you can set up daily syncing:

```bash
# Run sync once immediately
python daily_sync.py --run-once

# Or run as a scheduled service (runs at 2 AM daily)
python daily_sync.py
```

## Quick Commands Reference

```bash
# Check API status
python check_api.py

# Index database (first time)
python indexer.py --clear

# Index database (update)
python indexer.py

# Start the web chatbot
python app.py

# Daily sync (once)
python daily_sync.py --run-once
```

## Next Steps

1. ✅ Install dependencies: `pip install -r requirements.txt`
2. ✅ Configure `.env` file with your OpenAI API key
3. ✅ Test API: `python check_api.py`
4. ✅ Index database: `python indexer.py --clear`
5. ✅ Start the chatbot: `python app.py`

Happy querying! 🚀

