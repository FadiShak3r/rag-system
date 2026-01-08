"""
Embedding generation using OpenAI
"""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI, RateLimitError, APIError
from typing import List, Dict, Any, Optional
from config import OPENAI_API_KEY, OPENAI_EMBEDDING_MODEL


class EmbeddingGenerator:
    """Generates embeddings using OpenAI API with retry logic and rate limiting"""
    
    # Max tokens for text-embedding-3-small is 8191
    # Using ~4 chars per token as rough estimate, limit to ~30000 chars to be safe
    MAX_TEXT_LENGTH = 30000
    
    def __init__(self, batch_size: int = 100, delay_between_batches: float = 0.0, 
                 max_workers: int = 3, use_concurrent: bool = True):
        """
        Initialize EmbeddingGenerator with optimized settings
        
        Args:
            batch_size: Number of texts per API call (OpenAI supports up to 2048)
            delay_between_batches: Delay in seconds between batches (0 for no delay)
            max_workers: Number of concurrent threads for batch processing
            use_concurrent: Whether to use concurrent batch processing
        """
        if not OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.model = OPENAI_EMBEDDING_MODEL
        # OpenAI allows up to 2048 texts per batch for embeddings
        self.batch_size = min(batch_size, 2048)
        self.delay_between_batches = delay_between_batches
        self.max_workers = max_workers
        self.use_concurrent = use_concurrent
        self.max_retries = 5
        self.base_delay = 0.5
    
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
    
    def _process_single_batch(self, batch: List[str], batch_num: int, total_batches: int) -> tuple[int, List[List[float]]]:
        """Process a single batch of texts and return embeddings"""
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                response = self.client.embeddings.create(
                    model=self.model,
                    input=batch
                )
                batch_embeddings = [item.embedding for item in response.data]
                print(f"✓ Generated embeddings for batch {batch_num}/{total_batches} ({len(batch)} texts)")
                return batch_num, batch_embeddings
                
            except RateLimitError as e:
                retry_count += 1
                if retry_count < self.max_retries:
                    wait_time = self.base_delay * (2 ** retry_count)
                    print(f"⚠ Rate limit hit on batch {batch_num}. Waiting {wait_time:.1f} seconds before retry {retry_count}/{self.max_retries}...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Rate limit exceeded after {self.max_retries} retries on batch {batch_num}. "
                                  f"Please wait and try again later.")
                
            except APIError as e:
                if "insufficient_quota" in str(e).lower():
                    raise Exception(f"OpenAI API quota exceeded at batch {batch_num}. "
                                  f"Please check your plan and billing details at https://platform.openai.com/account/billing")
                raise
                
            except Exception as e:
                print(f"Error generating embeddings for batch {batch_num}: {e}")
                raise
        
        raise Exception(f"Failed to process batch {batch_num} after {self.max_retries} retries")
    
    def generate_embeddings_batch(self, texts: List[str], start_index: int = 0) -> List[List[float]]:
        """Generate embeddings for multiple texts in batches with retry logic and optional concurrent processing"""
        # Truncate all texts first
        texts = [self.truncate_text(t) for t in texts]
        
        total_batches = (len(texts) - 1) // self.batch_size + 1
        start_time = time.time()
        
        if self.use_concurrent and total_batches > 1:
            # Concurrent processing for multiple batches
            return self._generate_embeddings_concurrent(texts, start_index, total_batches, start_time)
        else:
            # Sequential processing
            return self._generate_embeddings_sequential(texts, start_index, total_batches, start_time)
    
    def _generate_embeddings_sequential(self, texts: List[str], start_index: int, 
                                        total_batches: int, start_time: float) -> List[List[float]]:
        """Generate embeddings sequentially"""
        all_embeddings = [None] * len(texts)
        
        for i in range(start_index, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            
            _, batch_embeddings = self._process_single_batch(batch, batch_num, total_batches)
            
            # Store embeddings at correct positions
            for idx, emb in enumerate(batch_embeddings):
                all_embeddings[i + idx] = emb
            
            # Progress update
            processed = min(i + self.batch_size, len(texts))
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            remaining = (len(texts) - processed) / rate if rate > 0 else 0
            print(f"  Progress: {processed}/{len(texts)} ({processed*100//len(texts)}%) | "
                  f"Rate: {rate:.1f} texts/sec | ETA: {remaining:.0f}s")
            
            if i + self.batch_size < len(texts) and self.delay_between_batches > 0:
                time.sleep(self.delay_between_batches)
        
        return all_embeddings
    
    def _generate_embeddings_concurrent(self, texts: List[str], start_index: int, 
                                       total_batches: int, start_time: float) -> List[List[float]]:
        """Generate embeddings using concurrent processing"""
        all_embeddings = [None] * len(texts)
        completed_batches = 0
        
        # Create batches with their indices
        batches = []
        for i in range(start_index, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            batches.append((i, batch, batch_num))
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all batches
            future_to_batch = {
                executor.submit(self._process_single_batch, batch, batch_num, total_batches): (start_idx, batch_num)
                for start_idx, batch, batch_num in batches
            }
            
            # Process completed batches as they finish
            for future in as_completed(future_to_batch):
                start_idx, batch_num = future_to_batch[future]
                try:
                    _, batch_embeddings = future.result()
                    
                    # Store embeddings at correct positions
                    for idx, emb in enumerate(batch_embeddings):
                        all_embeddings[start_idx + idx] = emb
                    
                    completed_batches += 1
                    processed = min(start_idx + len(batch_embeddings), len(texts))
                    elapsed = time.time() - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    remaining = (len(texts) - processed) / rate if rate > 0 else 0
                    print(f"  Progress: {processed}/{len(texts)} ({processed*100//len(texts)}%) | "
                          f"Rate: {rate:.1f} texts/sec | ETA: {remaining:.0f}s | "
                          f"Batches: {completed_batches}/{total_batches}")
                    
                except Exception as e:
                    print(f"❌ Error processing batch {batch_num}: {e}")
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
            print(f"Generating embeddings for {len(texts_to_process)} documents...")
            print(f"Batch size: {self.batch_size}, Concurrent: {self.use_concurrent}, Workers: {self.max_workers}")
            
            embeddings = self.generate_embeddings_batch(texts_to_process, start_index=start_index)
            
            # Assign embeddings to chunks
            for idx, embedding in enumerate(embeddings):
                chunk_idx = start_index + idx
                chunks[chunk_idx]['embedding'] = embedding
        else:
            print("All chunks already have embeddings")
        
        return chunks

