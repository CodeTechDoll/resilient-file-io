# Resilient File IO

Resilient File IO is a Python library that provides a set of utility functions to handle file input/output operations with resilience against interruptions or errors. It includes features such as saving progress, hashing, and resuming from checkpoints, which can be useful in a variety of applications.

## Features

- Read and write file operations with support for UTF-8 encoding
- Generation and verification of file content hashes (SHA-256)
- Progress checkpointing for resuming interrupted file writes
- Automatic resumption of file processing from the last checkpoint

## Installation

No external dependencies are required for this library. Simply clone the repository or copy the `file_processing.py` file into your project.

## Usage

```python
from file_processing import read_file, write_file, generate_hash, read_hash, write_hash, save_progress, load_progress, save_with_checkpoints, resume_checkpoint

# Read and write files
content = read_file('input.txt')
write_file('output.txt', content)

# Generate and verify content hashes
content_hash = generate_hash(content)
write_hash('content_hash.txt', content_hash)
saved_hash = read_hash('content_hash.txt')
print("Hashes match:", content_hash == saved_hash)

# Save and load progress
progress = {'last_saved': 0, 'total_size': len(content)}
save_progress('progress.json', progress)
loaded_progress = load_progress('progress.json')

# Save with checkpoints
progress_fraction = save_with_checkpoints('output_checkpoint.txt', content)
print(f"Progress: {progress_fraction * 100}%")

# Resume from checkpoint
def process_function(input_file):
    # Implement your processing function here
    return processed_content

resume_checkpoint('input.txt', 'output_checkpoint.txt', 'content_hash.txt', 'progress.json', process_function)
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

MIT
