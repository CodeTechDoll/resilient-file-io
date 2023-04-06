import hashlib
import json
import multiprocessing as mp
from multiprocessing import pool
import os
import mmap


def read_file_mmap(filepath, mode='r'):
    with open(filepath, mode) as f:
        mmapped = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    return mmapped


def write_file_mmap(filepath, content, mode='w'):
    with open(filepath, mode) as f:
        mmapped = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE)
        mmapped.write(content.encode())
        mmapped.close()


def generate_hash(content, encoding='utf-8'):
    sha256 = hashlib.sha256()
    sha256.update(content.encode(encoding))
    return sha256.hexdigest()


def read_hash(filepath, encoding='utf-8'):
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding=encoding) as f:
            return f.read().strip()
    return None


def write_hash(filepath, hash_value, encoding='utf-8'):
    with open(filepath, 'w', encoding=encoding) as f:
        f.write(hash_value)


def save_progress(progress_file, progress, encoding='utf-8'):
    with open(progress_file, 'w', encoding=encoding) as f:
        json.dump(progress, f)


def load_progress(progress_file, encoding='utf-8'):
    if os.path.exists(progress_file):
        with open(progress_file, 'r', encoding=encoding) as f:
            return json.load(f)
    return None


def save_with_checkpoints_mmap(filepath, content, checkpoint_size=1024, encoding='utf-8'):
    total_size = len(content)
    last_saved = 0

    if os.path.exists(filepath):
        saved_content = read_file_mmap(filepath)
        last_saved = len(saved_content)

    write_file_mmap(filepath, content[last_saved:], 'a')

    return last_saved + len(content[last_saved:])  # Return the actual saved position


def process_chunk(chunk, processing_function, output_dict):
    chunk_size = len(chunk)
    sub_chunk_size = 10000  # Process the chunk in sub-chunks of 10,000 characters
    processed_chunk = ''
    for i in range(0, chunk_size, sub_chunk_size):
        sub_chunk = chunk[i:i+sub_chunk_size]
        processed_sub_chunk = processing_function(sub_chunk)
        processed_chunk += processed_sub_chunk

    output_dict[chunk] = processed_chunk  # Save the processed chunk to the shared dictionary


def resume_checkpoint(input_file, output_file, hash_file, progress_file, processing_function, encoding='utf-8', num_workers=4):
    progress = load_progress(progress_file, encoding)

    if progress is None:
        progress = {
            'last_saved': 0,
            'total_size': None
        }

    try:
        with open(input_file, 'r+b') as f:
            processed_content = mmap.mmap(f.fileno(), 0)
            if progress['total_size'] is None:
                progress['total_size'] = len(processed_content)
                save_progress(progress_file, progress, encoding)

            with mp.Manager() as manager:
                pool_results = []
                output_dict = manager.dict()

                chunk_size = progress['total_size'] // num_workers
                start_positions = [i * chunk_size for i in range(num_workers)]
                end_positions = start_positions[1:] + [progress['total_size']]

                for i in range(num_workers):
                    start_pos = start_positions[i]
                    end_pos = end_positions[i]
                    if start_pos >= progress['last_saved']:
                        pool_results.append(pool.apply_async(process_chunk, (processed_content[start_pos:end_pos], processing_function, output_dict)))
                    else:
                        pool_results.append(None)

                while progress['last_saved'] < progress['total_size']:
                    for i in range(num_workers):
                        if pool_results[i] and pool_results[i].ready():
                            pool_results[i].get()
                            pool_results[i] = None
                            output_pos = save_with_checkpoints(output_file, output_dict, end_positions[i], encoding)
                            if i == num_workers - 1 or output_pos >= end_positions[i]:
                                progress['last_saved'] = end_positions[i]
                                save_progress(progress_file, progress, encoding)
                                if progress['last_saved'] >= progress['total_size']:
                                    # Compute and save the hash of the processed content to the hash file
                                    hash_value = generate_hash(processed_content[:progress['total_size']], encoding)
                                    write_hash(hash_file, hash_value, encoding)
                                    print("Finished processing and saved the output.")
                                    os.remove(progress_file)  # Remove the progress file when the process is completed successfully
                            else:
                                pool_results[i] = pool.apply_async(process_chunk, (processed_content[end_positions[i]:start_positions[i+1]], processing_function, output_dict))

            pool.close()
            pool.join()

    except Exception as e:
        print(f"Error occurred: {e}. Please restart the script to resume.")