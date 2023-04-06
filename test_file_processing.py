import unittest
import os
import hashlib
import json
from file_processing import read_file, save_with_checkpoints, generate_hash, write_hash, load_progress, save_progress, resume_checkpoint

def processing_function(chunk):
    return chunk.upper()


class TestFileProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.test_dir = os.path.dirname(os.path.abspath(__file__))
        self.test_input_file = os.path.join(self.test_dir, 'test_input.txt')
        self.test_output_file = os.path.join(self.test_dir, 'test_output.txt')
        self.test_hash_file = os.path.join(self.test_dir, 'test_hash.txt')
        self.test_progress_file = os.path.join(self.test_dir, 'test_progress.json')

        # Create a test input file with some content
        with open(self.test_input_file, 'w', encoding='utf-8') as f:
            f.write('Hello, world!\n' * 1000)

        # Create an empty test output file
        open(self.test_output_file, 'w').close()

        # Initialize test progress file
        progress = {
            'last_saved': 0,
            'total_size': None
        }
        with open(self.test_progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress, f)

    @classmethod
    def tearDownClass(cls):
        # Remove the test files created in setUpClass()
        os.remove(cls.test_input_file)
        os.remove(cls.test_output_file)
        os.remove(cls.test_hash_file)
        os.remove(cls.test_progress_file)

    def test_read_file(self):
        content = read_file(self.test_input_file)
        self.assertEqual(content, 'Hello, world!\n' * 1000)

    def test_save_with_checkpoints(self):
        content = 'Hello, world!\n' * 1000
        save_with_checkpoints(self.test_output_file, content)
        with open(self.test_output_file, 'r', encoding='utf-8') as f:
            saved_content = f.read()
        self.assertEqual(saved_content, content)

    def test_generate_hash(self):
        content = 'Hello, world!\n' * 1000
        hash_value = generate_hash(content)
        self.assertEqual(hash_value, hashlib.sha256(content.encode('utf-8')).hexdigest())

    def test_write_hash(self):
        hash_value = 'abc123'
        write_hash(self.test_hash_file, hash_value)
        with open(self.test_hash_file, 'r', encoding='utf-8') as f:
            saved_hash = f.read().strip()
        self.assertEqual(saved_hash, hash_value)

    def test_load_progress(self):
        progress = load_progress(self.test_progress_file)
        self.assertEqual(progress, {'last_saved': 0, 'total_size': None})

    def test_save_progress(self):
        progress = {'last_saved': 100, 'total_size': 1000}
        save_progress(self.test_progress_file, progress)
        with open(self.test_progress_file, 'r', encoding='utf-8') as f:
            saved_progress = json.load(f)
        self.assertEqual(saved_progress, progress)

    def test_resume_checkpoint(self):
        content = "This is a test content."
        with open(self.test_input_file, "w") as f:
            f.write(content)

        # Case 1: Process file with 1 worker
        num_workers = 1
        progress = {
            'last_saved': 0,
            'total_size': len(content)
        }
        with open(self.test_progress_file, 'w') as f:
            f.write(json.dumps(progress))

        resume_checkpoint(self.test_input_file, self.test_output_file, self.test_hash_file, self.test_progress_file, processing_function, num_workers=num_workers)

        expected_output = content.upper()
        with open(self.test_output_file, 'r') as f:
            actual_output = f.read()
        self.assertEqual(expected_output, actual_output)

        expected_hash = hashlib.sha256(expected_output.encode('utf-8')).hexdigest()
        with open(self.test_hash_file, 'r') as f:
            actual_hash = f.read().strip()
        self.assertEqual(expected_hash, actual_hash)

        self.assertFalse(os.path.exists(self.test_progress_file))

        # Case 2: Resume processing with 2 workers
        num_workers = 2
        with open(self.test_progress_file, 'r') as f:
            progress = json.load(f)
        resume_checkpoint(self.test_input_file, self.test_output_file, self.test_hash_file, self.test_progress_file, processing_function, num_workers=num_workers)

        expected_output = content.upper() * 2
        with open(self.test_output_file, 'r') as f:
            actual_output = f.read()
        self.assertEqual(expected_output, actual_output)

        expected_hash = hashlib.sha256(expected_output.encode('utf-8')).hexdigest()
        with open(self.test_hash_file, 'r') as f:
            actual_hash = f.read().strip()
        self.assertEqual(expected_hash, actual_hash)

        self.assertFalse(os.path.exists(self.test_progress_file))

