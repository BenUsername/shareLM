"""
Migration script to pipe Hugging Face ShareLM dataset to MongoDB
Run this script to populate MongoDB with data from the Hugging Face dataset
"""
import os
import sys
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import duckdb
from huggingface_hub import HfFileSystem
from datetime import datetime
from dotenv import load_dotenv
import numpy as np
import pandas as pd

# Load environment variables
load_dotenv()

# MongoDB connection
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
DATABASE_NAME = os.getenv('MONGODB_DATABASE', 'sharelm')
COLLECTION_NAME = os.getenv('MONGODB_COLLECTION', 'conversations')

# Initialize Hugging Face filesystem
fs = HfFileSystem()

def get_parquet_urls():
    """Get actual HTTP URLs for parquet files from Hugging Face"""
    try:
        parquet_files = []
        patterns = [
            "datasets/shachardon/ShareLM/*/train/*.parquet",
            "datasets/shachardon/ShareLM/**/*.parquet",
            "datasets/shachardon/ShareLM/**/train/*.parquet"
        ]
        
        for pattern in patterns:
            try:
                files = fs.glob(pattern)
                if files:
                    parquet_files = files
                    break
            except Exception as e:
                print(f"Error with pattern {pattern}: {e}")
                continue
        
        if not parquet_files:
            raise Exception("No parquet files found with any pattern")
        
        # Convert to HTTP URLs
        urls = []
        for file_path in parquet_files:
            try:
                url = fs.url(file_path)
                if url and url.startswith('http'):
                    urls.append(url)
            except Exception as e:
                print(f"Error converting {file_path} to URL: {e}")
                continue
        
        if urls:
            return urls
        else:
            raise Exception("No valid parquet URLs found")
    except Exception as e:
        print(f"Error getting parquet URLs: {e}")
        raise

def fetch_single_file(url):
    """Fetch data from a single parquet file"""
    try:
        con = duckdb.connect()
        
        # Install and load httpfs extension for remote file access
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        
        escaped_url = url.replace("'", "''")
        query = f"SELECT * FROM read_parquet('{escaped_url}')"
        result = con.execute(query).fetchdf()
        con.close()
        
        return result
    except Exception as e:
        print(f"Error fetching file {url}: {e}")
        raise

def get_processed_files(collection):
    """Get set of already processed file URLs"""
    try:
        # Use a metadata collection to track processed files
        metadata_coll = collection.database['_migration_metadata']
        processed = metadata_coll.find_one({'_id': 'processed_files'})
        if processed:
            return set(processed.get('files', []))
        return set()
    except:
        return set()

def mark_file_processed(collection, file_url):
    """Mark a file as processed"""
    try:
        metadata_coll = collection.database['_migration_metadata']
        metadata_coll.update_one(
            {'_id': 'processed_files'},
            {'$addToSet': {'files': file_url}},
            upsert=True
        )
    except Exception as e:
        print(f"Warning: Could not mark file as processed: {e}")

def prepare_document(row_dict):
    """Prepare a document for MongoDB insertion"""
    import numpy as np
    import pandas as pd
    
    doc = dict(row_dict)
    
    # Add metadata
    doc['_imported_at'] = datetime.utcnow()
    
    # Recursively convert non-serializable types
    def convert_value(value):
        """Convert value to MongoDB-compatible type"""
        # Handle numpy arrays
        if isinstance(value, np.ndarray):
            return value.tolist()
        # Handle numpy scalars
        if isinstance(value, (np.integer, np.floating)):
            return value.item()
        # Handle numpy bool
        if isinstance(value, np.bool_):
            return bool(value)
        # Handle pandas NaT (Not a Time)
        if pd.isna(value):
            return None
        # Handle datetime-like strings
        if isinstance(value, str):
            # Try to parse ISO format dates
            if 'T' in value or (value.count('-') >= 2 and len(value) >= 10):
                try:
                    # Try ISO format
                    if 'T' in value:
                        dt = datetime.fromisoformat(value.replace('Z', '+00:00').split('.')[0])
                        return dt
                    elif len(value) == 10 and value.count('-') == 2:
                        # Date only
                        dt = datetime.strptime(value, '%Y-%m-%d')
                        return dt
                except:
                    pass
        # Handle lists (recursively convert items)
        if isinstance(value, list):
            return [convert_value(item) for item in value]
        # Handle dicts (recursively convert values)
        if isinstance(value, dict):
            return {k: convert_value(v) for k, v in value.items()}
        # Handle pandas Timestamp
        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return None
            return value.to_pydatetime()
        # Return as-is if already serializable
        return value
    
    # Convert all values in the document
    for key, value in doc.items():
        doc[key] = convert_value(value)
    
    return doc

def migrate_to_mongodb(max_files=None, batch_size=1000, resume=True):
    """Migrate data from Hugging Face to MongoDB, processing files one at a time"""
    try:
        # Connect to MongoDB
        print(f"Connecting to MongoDB at {MONGODB_URI}...")
        client = MongoClient(MONGODB_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        
        # Create indexes for better query performance
        print("Creating indexes...")
        collection.create_index("source")
        collection.create_index([("timestamp", 1)])
        collection.create_index([("date", 1)])
        collection.create_index([("created_at", 1)])
        collection.create_index([("_imported_at", 1)])
        
        # Get parquet file URLs
        parquet_urls = get_parquet_urls()
        if not parquet_urls:
            raise Exception("No parquet files found")
        
        # Limit files if specified
        if max_files:
            parquet_urls = parquet_urls[:max_files]
        
        # Get already processed files if resuming
        processed_files = set()
        if resume:
            processed_files = get_processed_files(collection)
            print(f"Found {len(processed_files)} already processed files")
        
        # Filter out already processed files
        files_to_process = [url for url in parquet_urls if url not in processed_files]
        
        if not files_to_process:
            print("All files have already been processed!")
            print(f"Total documents in collection: {collection.count_documents({})}")
            client.close()
            return
        
        print(f"Processing {len(files_to_process)} parquet files ({len(processed_files)} already done)...")
        
        total_inserted = 0
        total_skipped = 0
        total_files_processed = 0
        
        # Process files one at a time to manage memory
        for file_idx, url in enumerate(files_to_process):
            try:
                print(f"\n[{file_idx + 1}/{len(files_to_process)}] Processing: {url}")
                
                # Fetch single file
                df = fetch_single_file(url)
                
                if df.empty:
                    print(f"  File is empty, skipping...")
                    mark_file_processed(collection, url)
                    continue
                
                print(f"  Fetched {len(df)} rows")
                
                # Convert DataFrame to list of documents and insert in batches
                documents = []
                file_inserted = 0
                file_skipped = 0
                
                for idx, row in df.iterrows():
                    doc = prepare_document(row.to_dict())
                    documents.append(doc)
                    
                    # Insert in batches
                    if len(documents) >= batch_size:
                        try:
                            result = collection.insert_many(documents, ordered=False)
                            inserted_count = len(result.inserted_ids)
                            file_inserted += inserted_count
                            total_inserted += inserted_count
                        except BulkWriteError as e:
                            inserted_count = e.details.get('nInserted', 0)
                            file_inserted += inserted_count
                            file_skipped += len(documents) - inserted_count
                            total_inserted += inserted_count
                            total_skipped += len(documents) - inserted_count
                        except Exception as e:
                            print(f"  Error inserting batch: {e}")
                            file_skipped += len(documents)
                            total_skipped += len(documents)
                        
                        documents = []
                        if file_inserted % (batch_size * 10) == 0:
                            print(f"  Progress: {file_inserted} inserted from this file")
                
                # Insert remaining documents
                if documents:
                    try:
                        result = collection.insert_many(documents, ordered=False)
                        inserted_count = len(result.inserted_ids)
                        file_inserted += inserted_count
                        total_inserted += inserted_count
                    except BulkWriteError as e:
                        inserted_count = e.details.get('nInserted', 0)
                        file_inserted += inserted_count
                        file_skipped += len(documents) - inserted_count
                        total_inserted += inserted_count
                        total_skipped += len(documents) - inserted_count
                    except Exception as e:
                        print(f"  Error inserting final batch: {e}")
                        file_skipped += len(documents)
                        total_skipped += len(documents)
                
                # Mark file as processed
                mark_file_processed(collection, url)
                total_files_processed += 1
                
                print(f"  File complete: {file_inserted} inserted, {file_skipped} skipped")
                print(f"  Total progress: {total_inserted} documents inserted, {total_skipped} skipped")
                
                # Clear memory
                del df
                del documents
                
            except Exception as e:
                print(f"  Error processing file {url}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"\n{'='*60}")
        print(f"Migration complete!")
        print(f"Files processed: {total_files_processed}/{len(files_to_process)}")
        print(f"Total documents inserted: {total_inserted}")
        print(f"Total documents skipped: {total_skipped}")
        print(f"Total documents in collection: {collection.count_documents({})}")
        print(f"{'='*60}")
        
        client.close()
        
    except Exception as e:
        print(f"Error during migration: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate ShareLM dataset from Hugging Face to MongoDB')
    parser.add_argument('--max-files', type=int, default=None, help='Maximum number of parquet files to process')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for MongoDB inserts')
    parser.add_argument('--no-resume', action='store_true', help='Do not resume from previous migration (reprocess all files)')
    
    args = parser.parse_args()
    
    print("Starting migration from Hugging Face to MongoDB...")
    print(f"Database: {DATABASE_NAME}")
    print(f"Collection: {COLLECTION_NAME}")
    print(f"Resume mode: {not args.no_resume}")
    print()
    
    migrate_to_mongodb(max_files=args.max_files, batch_size=args.batch_size, resume=not args.no_resume)

