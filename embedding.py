"""
Embedding generation using OpenAI
"""
import time
from openai import OpenAI, RateLimitError, APIError
from typing import List, Dict, Any, Optional
from config import OPENAI_API_KEY, OPENAI_EMBEDDING_MODEL


class EmbeddingGenerator:
    """Generates embeddings using OpenAI API with retry logic and rate limiting"""
    
    # Max tokens for text-embedding-3-small is 8191
    # Using ~4 chars per token as rough estimate, limit to ~30000 chars to be safe
    MAX_TEXT_LENGTH = 30000
    
    def __init__(self, batch_size: int = 10, delay_between_batches: float = 1.0):
        if not OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.model = OPENAI_EMBEDDING_MODEL
        self.batch_size = batch_size
        self.delay_between_batches = delay_between_batches
        self.max_retries = 5
        self.base_delay = 2.0
    
    def truncate_text(self, text: str) -> str:
        """Truncate text to fit within token limits"""
        if len(text) <= self.MAX_TEXT_LENGTH:
            return text
        
        # Truncate and add indicator
        truncated = text[:self.MAX_TEXT_LENGTH - 50]
        # Try to cut at last complete line
        last_newline = truncated.rfind('\n')
        if last_newline > self.MAX_TEXT_LENGTH - 500:
            truncated = truncated[:last_newline]
        
        return truncated + "\n\n[Content truncated due to length]"
    
    def generate_embedding(self, text: str, retry_count: int = 0) -> List[float]:
        """Generate embedding for a single text with retry logic"""
        # Truncate if too long
        text = self.truncate_text(text)
        
        try:
            response = self.client.embeddings.create(
                model=self.model,
                input=text
            )
            return response.data[0].embedding
        except RateLimitError as e:
            if retry_count < self.max_retries:
                wait_time = self.base_delay * (2 ** retry_count)
                print(f"Rate limit hit. Waiting {wait_time:.1f} seconds before retry {retry_count + 1}/{self.max_retries}...")
                time.sleep(wait_time)
                return self.generate_embedding(text, retry_count + 1)
            else:
                raise Exception(f"Rate limit exceeded after {self.max_retries} retries. Please check your OpenAI quota and billing.")
        except APIError as e:
            if "insufficient_quota" in str(e).lower():
                raise Exception("OpenAI API quota exceeded. Please check your plan and billing details at https://platform.openai.com/account/billing")
            raise
        except Exception as e:
            print(f"Error generating embedding: {e}")
            raise
    
    def generate_embeddings_batch(self, texts: List[str], start_index: int = 0) -> List[List[float]]:
        """Generate embeddings for multiple texts in batches with retry logic"""
        # Truncate all texts first
        texts = [self.truncate_text(t) for t in texts]
        
        all_embeddings = []
        total_batches = (len(texts) - 1) // self.batch_size + 1
        
        for i in range(start_index, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            
            retry_count = 0
            while retry_count < self.max_retries:
                try:
                    response = self.client.embeddings.create(
                        model=self.model,
                        input=batch
                    )
                    batch_embeddings = [item.embedding for item in response.data]
                    all_embeddings.extend(batch_embeddings)
                    print(f"✓ Generated embeddings for batch {batch_num}/{total_batches} ({len(batch)} texts)")
                    
                    if i + self.batch_size < len(texts):
                        time.sleep(self.delay_between_batches)
                    
                    break
                    
                except RateLimitError as e:
                    retry_count += 1
                    if retry_count < self.max_retries:
                        wait_time = self.base_delay * (2 ** retry_count)
                        print(f"⚠ Rate limit hit on batch {batch_num}. Waiting {wait_time:.1f} seconds before retry {retry_count}/{self.max_retries}...")
                        time.sleep(wait_time)
                    else:
                        raise Exception(f"Rate limit exceeded after {self.max_retries} retries on batch {batch_num}. "
                                      f"Progress: {len(all_embeddings)}/{len(texts)} embeddings generated. "
                                      f"Please wait and try again later.")
                    
                except APIError as e:
                    if "insufficient_quota" in str(e).lower():
                        raise Exception(f"OpenAI API quota exceeded at batch {batch_num}. "
                                      f"Progress: {len(all_embeddings)}/{len(texts)} embeddings generated. "
                                      f"Please check your plan and billing details at https://platform.openai.com/account/billing")
                    raise
                    
                except Exception as e:
                    print(f"Error generating embeddings for batch {batch_num}: {e}")
                    raise
        
        return all_embeddings
    
    def add_embeddings_to_chunks(self, chunks: List[Dict[str, Any]], 
                                 resume_from: Optional[int] = None) -> List[Dict[str, Any]]:
        """Add embeddings to chunk dictionaries with optional resume capability"""
        texts = [chunk['text'] for chunk in chunks]
        
        start_index = resume_from if resume_from is not None else 0
        
        if start_index > 0:
            print(f"Resuming from chunk {start_index}/{len(chunks)}")
            texts_to_process = texts[start_index:]
        else:
            texts_to_process = texts
        
        if texts_to_process:
            embeddings = self.generate_embeddings_batch(texts_to_process, start_index=start_index)
            
            for idx, embedding in enumerate(embeddings):
                chunk_idx = start_index + idx
                chunks[chunk_idx]['embedding'] = embedding
        else:
            print("All chunks already have embeddings")
        
        return chunks

