"""
Product Database Assistant - Web Chatbot Interface
"""
import requests
from flask import Flask, render_template, request, jsonify
from rag_system import RAGSystem
from config import META_BASE_URL, META_BASE_API_KEY
import sys

app = Flask(__name__)


def normalize_metabase_url(url):
    """Ensure Metabase URL has a protocol"""
    if not url:
        return url
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        # Default to http if no protocol specified
        url = f'http://{url}'
    # Remove trailing slash
    return url.rstrip('/')

# Initialize RAG system
print("\n" + "=" * 50)
print("PRODUCT DATABASE ASSISTANT")
print("=" * 50)
print("\nInitializing...")

try:
    rag = RAGSystem()
    stats = rag.get_stats()
    doc_count = stats.get('document_count', 'unknown')
    print(f"\n✓ Ready! ({doc_count} documents indexed)")
    print(f"\n🌐 Open http://localhost:4100 in your browser")
    print("=" * 50 + "\n")
except Exception as e:
    print(f"\n❌ Error: {e}")
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


@app.route('/api/card', methods=['POST'])
def create_metabase_card():
    """Create a Metabase card from a question"""
    try:
        data = request.json
        question = data.get('question', '').strip()
        
        if not question:
            return jsonify({'error': 'Please provide a question'}), 400
        
        if not META_BASE_API_KEY:
            return jsonify({'error': 'Metabase API key not configured'}), 500
        
        # Generate card configuration using AI
        card_config = rag.generate_metabase_card(question)
        
        if not card_config:
            return jsonify({'error': 'Could not generate card configuration'}), 500
        
        # Create the card in Metabase
        headers = {
            'Content-Type': 'application/json',
            'X-API-KEY': META_BASE_API_KEY
        }
        
        metabase_url = normalize_metabase_url(META_BASE_URL)
        response = requests.post(
            f"{metabase_url}/api/card",
            headers=headers,
            json=card_config
        )


        # Check if response is HTML (likely an error page or login redirect)
        content_type = response.headers.get('Content-Type', '').lower()
        if 'text/html' in content_type or response.text.strip().startswith('<!'):
            return jsonify({
                'error': 'Metabase returned HTML instead of JSON. This usually means:\n'
                         '1. The API endpoint URL is incorrect\n'
                         '2. Authentication failed (check API key)\n'
                         '3. Metabase redirected to a login page\n\n'
                         f'Response preview: {response.text[:200]}...',
                'status_code': response.status_code,
                'url': f"{META_BASE_URL}/api/card"
            }), 500
        
        if response.status_code not in [200, 201]:
            try:
                error_data = response.json()
                error_msg = error_data.get('message', error_data.get('error', response.text))
            except:
                error_msg = response.text[:500]
            
            return jsonify({
                'error': f'Metabase API error: {error_msg}',
                'status_code': response.status_code,
                'card_config': card_config
            }), response.status_code
        
        try:
            card_data = response.json()
        except ValueError as e:
            return jsonify({
                'error': f'Invalid JSON response from Metabase: {str(e)}\n'
                         f'Response: {response.text[:500]}',
                'status_code': response.status_code
            }), 500
        
        card_id = card_data.get('id')
        
        if not card_id:
            return jsonify({'error': 'Card created but no ID returned'}), 500
        
        return jsonify({
            'success': True,
            'card_id': card_id,
            'card_name': card_data.get('name'),
            'card_config': card_config
        })
        
    except requests.RequestException as e:
        return jsonify({'error': f'Network error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/metabase/public-link/<int:card_id>', methods=['POST'])
def create_public_link(card_id):
    """Create a public link for a Metabase card"""
    try:
        if not META_BASE_API_KEY:
            return jsonify({'error': 'Metabase API key not configured'}), 500
        
        headers = {
            'Content-Type': 'application/json',
            'X-API-KEY': META_BASE_API_KEY
        }
        
        metabase_url = normalize_metabase_url(META_BASE_URL)
        print(f"{metabase_url}/api/card/{card_id}/public_link")
        response = requests.post(
            f"{metabase_url}/api/card/{card_id}/public_link",
            headers=headers
        )
        
        # Check if response is HTML (likely an error page or login redirect)
        content_type = response.headers.get('Content-Type', '').lower()
        if 'text/html' in content_type or response.text.strip().startswith('<!'):
            return jsonify({
                'error': 'Metabase returned HTML instead of JSON. This usually means:\n'
                         '1. The API endpoint URL is incorrect\n'
                         '2. Authentication failed (check API key)\n'
                         '3. Metabase redirected to a login page\n\n'
                         f'Response preview: {response.text[:200]}...',
                'status_code': response.status_code,
                'url': f"{metabase_url}/api/card/{card_id}/public_link"
            }), 500
        
        if response.status_code not in [200, 201]:
            try:
                error_data = response.json()
                error_msg = error_data.get('message', error_data.get('error', response.text))
            except:
                error_msg = response.text[:500]
            
            return jsonify({
                'error': f'Metabase API error: {error_msg}',
                'status_code': response.status_code
            }), response.status_code
        
        try:
            link_data = response.json()
        except ValueError as e:
            return jsonify({
                'error': f'Invalid JSON response from Metabase: {str(e)}\n'
                         f'Response: {response.text[:500]}',
                'status_code': response.status_code
            }), 500
        
        uuid = link_data.get('uuid')
        
        if not uuid:
            return jsonify({'error': 'Public link created but no UUID returned'}), 500
        
        public_url = f"{metabase_url}/public/question/{uuid}"
        
        return jsonify({
            'success': True,
            'uuid': uuid,
            'public_url': public_url
        })
        
    except requests.RequestException as e:
        return jsonify({'error': f'Network error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/metabase/config', methods=['GET'])
def get_metabase_config():
    """Get Metabase configuration (public info only)"""
    return jsonify({
        'base_url': normalize_metabase_url(META_BASE_URL),
        'configured': bool(META_BASE_API_KEY)
    })


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=4100)

