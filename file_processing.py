from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
import mmap


def read_file_mmap(filepath, mode='r', encoding='utf-8'):
    with open(filepath, mode, encoding=encoding) as f:
        file_size = os.stat(filepath).st_size
        if file_size == 0:
            return ""
        mmapped = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            return mmapped.read().decode(encoding)
        finally:
            mmapped.close()

def write_file_mmap(filepath, content, mode='w', encoding='utf-8'):
    with open(filepath, mode, encoding=encoding) as f:
        mmapped = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE)
        try:
            mmapped.write(content.encode(encoding))
        finally:
            mmapped.close()


def generate_hash(content, encoding='utf-8'):
    sha256 = hashlib.sha256()
    sha256.update(content.encode(encoding))
    return sha256.hexdigest()


def load_hash(filepath, encoding='utf-8'):
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding=encoding) as f:
            return f.read().strip()
    return None


def save_hash(filepath, hash_value, encoding='utf-8'):
    with open(filepath, 'w', encoding=encoding) as f:
        f.write(hash_value)


def save_progress(progress_file, progress, encoding='utf-8'):
    with open(progress_file, 'w', encoding=encoding) as f:
        json.dump(progress, f)

def load_progress(progress_file, encoding='utf-8'):
    if not os.path.exists(progress_file):
        return None
    with open(progress_file, 'r', encoding=encoding) as f:
        content = f.read()
        if not content:
            return None
        return json.loads(content)

def save_with_checkpoints(filepath, content, processing_function, checkpoint_size=1024, encoding='utf-8', num_workers=4):
    total_size = len(content)
    last_saved = 0

    # Read the saved content from the file and update the last_saved position
    if os.path.exists(filepath):
        saved_content = read_file_mmap(filepath)
        last_saved = len(saved_content)

    # Open the output file in append mode
    with open(filepath, 'a', encoding=encoding) as f:
        # Create a ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Continue processing while the last_saved position is less than the total_size of the content
            while last_saved < total_size:
                end_pos = min(last_saved + checkpoint_size, total_size)
                
                # Create sub-chunks of content to be processed in parallel
                sub_chunks = [(content[i:i + checkpoint_size], processing_function) for i in range(last_saved, end_pos, checkpoint_size)]
                
                # Use the executor to map the process_sub_chunk function to the sub_chunks
                results = list(executor.map(lambda args: process_sub_chunk(*args), sub_chunks))

                # Write the processed results to the output file
                for result in results:
                    f.write(result)

                # Update the last_saved position
                last_saved = end_pos

    # Return the actual saved position
    return last_saved

def process_chunk(chunk, processing_function, output_dict):
    chunk_size = len(chunk)
    sub_chunk_size = 10000  # Process the chunk in sub-chunks of 10,000 characters
    processed_chunk = ''
    for i in range(0, chunk_size, sub_chunk_size):
        sub_chunk = chunk[i:i+sub_chunk_size]
        processed_sub_chunk = processing_function(sub_chunk)
        processed_chunk += processed_sub_chunk

    output_dict[chunk] = processed_chunk  # Save the processed chunk to the shared dictionary

def process_sub_chunk(sub_chunk, processing_function):
    return processing_function(sub_chunk)

def resume_checkpoint(input_file, output_file, hash_file, progress_file, processing_function):
    # Load the progress file
    progress = load_progress(progress_file)

    # If progress is `None`, it means that the progress file doesn't exist so create a new progress dictionary
    if progress is None:
        progress = {
            'last_saved': 0,
            'total_size': None
        }

    try:
        processed_content = processing_function(input_file) # Process the content from the input file using the provided `processing_function`

        # Generate the hash of the processed content
        content_hash = generate_hash(processed_content) 
        saved_hash = load_hash(hash_file) # Load the saved hash from the hash file


        if saved_hash is None: # Check if the saved hash is `None` or if it matches the generated hash

            save_hash(hash_file, content_hash)
        elif saved_hash != content_hash: # If the saved hash doesn't match the generated hash, raise an error

            raise ValueError("Hash mismatch. Processed content might have been tampered with.")
        if progress['total_size'] is None: # If the saved hash matches the generated hash, continue processing the content


            progress['total_size'] = len(processed_content) # If the total size is `None`, it means that the progress file doesn't exist so save the total size
            save_progress(progress_file, progress)
            
        # Save the processed content to the output file
        while progress['last_saved'] < progress['total_size']: # While the last saved position is less than the total size, save the processed content to the output file
            progress['last_saved'] = save_with_checkpoints(output_file, processed_content, progress['last_saved']) # Save the processed content to the output file in chunks

            save_progress(progress_file, progress) # Save the progress to the progress file

            if progress['last_saved'] >= progress['total_size']: # Check if the last saved position is greater than or equal to the total size


                print("Finished processing and saved the output.") # If the last saved position is greater than or equal to the total size, it means that the processing is complete
                if os.path.exists(progress_file): # Check if the progress file exists before trying to remove it
                    os.remove(progress_file)
    except (FileNotFoundError, ValueError, IOError) as e: # Catch specific exceptions
        print(f"Error occurred: {e}. Please restart the script to resume.")