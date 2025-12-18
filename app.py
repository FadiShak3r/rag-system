from flask import Flask, render_template, request, jsonify
from rag_system import RAGSystem
import sys

app = Flask(__name__)

# Initialize RAG system
print("Initializing RAG system...")
try:
    print("Creating RAGSystem instance...")
    rag = RAGSystem()
    print("RAGSystem created.")
    # Skip stats retrieval during startup - it's not critical and can hang on large collections
    # Stats can be retrieved via /api/stats endpoint if needed
    print("✓ RAG system ready!")
    print("Starting Flask server on http://0.0.0.0:4100")
except Exception as e:
    print(f"Error initializing RAG system: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)


@app.route('/')
def index():
    """Render the chatbot interface"""
    return render_template('chatbot.html')


@app.route('/api/query', methods=['POST'])
def query():
    """Handle chatbot queries"""
    try:
        data = request.json
        question = data.get('question', '').strip()
        
        if not question:
            return jsonify({'error': 'Please provide a question'}), 400
        
        # Get answer from RAG system
        answer = rag.query(question)
        
        return jsonify({
            'answer': answer,
            'question': question
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stats', methods=['GET'])
def stats():
    """Get system statistics"""
    try:
        stats = rag.get_stats()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=4100)

