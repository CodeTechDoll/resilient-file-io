import multiprocessing
import unittest
import os
import shutil
import tempfile
import string
import random
from unittest import mock
from file_processing import *

def sample_processing_function(content):
    return content.upper()

class TestFileProcessing(unittest.TestCase):
    test_dir = 'test_dir'

    def setUp(self):
        os.makedirs(self.test_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_existing_file(self):
        content = "This is a test file."
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content.encode('utf-8'))
            f.flush()

        try:
            read_content = read_file_mmap(f.name)
            self.assertEqual(content, read_content)
        finally:
            os.remove(f.name)

    def test_read_empty_file(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.flush()

        try:
            read_content = read_file_mmap(f.name)
            self.assertEqual("", read_content)
        finally:
            os.remove(f.name)

    def test_read_nonexistent_file(self):
        with self.assertRaises(FileNotFoundError):
            read_file_mmap("nonexistent_file.txt")

    def test_read_file_with_utf8_chars(self):
        content = "Résumé: Café à côté de l'église."
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content.encode('utf-8'))
            f.flush()

        try:
            read_content = read_file_mmap(f.name)
            self.assertEqual(content, read_content)
        finally:
            os.remove(f.name)

    def test_read_file_with_utf16_chars(self):
        content = "Résumé: Café à côté de l'église."
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content.encode('utf-16'))
            f.flush()

        try:
            read_content = read_file_mmap(f.name, encoding='utf-16')
            self.assertEqual(content, read_content)
        finally:
            os.remove(f.name)

    def test_generate_hash(self):
        content = "Sample content"
        expected_hash = 'ca83c6acbe7f1270c63b0b4d0b2b180c347b6d5cab6e95b2fd7be152f345314b'
        self.assertEqual(expected_hash, generate_hash(content))

    def test_generate_hash_different_content(self):
        content = "Another sample content"
        expected_hash = 'f8cc3db3e96bcb0ba1e7c5e8a0ee4409c9f9e56dfab1a2e5d50b82c89b1e08a5'
        self.assertNotEqual(expected_hash, generate_hash(content))

    def test_load_save_hash(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            filepath = f.name
        try:
            hash_value = "7cfa8d90626d7b20c0b1a7d2058d25312f7bdfc6baf81f3c8f3db3d4b4c4a41e"
            save_hash(filepath, hash_value)

            loaded_hash = load_hash(filepath)
            self.assertEqual(hash_value, loaded_hash)
        finally:
            os.remove(filepath)

    def test_load_hash_nonexistent_file(self):
        self.assertIsNone(load_hash("nonexistent_file.txt"))

    def test_save_hash_overwrite(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            filepath = f.name
        try:
            initial_hash = "7cfa8d90626d7b20c0b1a7d2058d25312f7bdfc6baf81f3c8f3db3d4b4c4a41e"
            new_hash = "f8cc3db3e96bcb0ba1e7c5e8a0ee4409c9f9e56dfab1a2e5d50b82c89b1e08a5"
            save_hash(filepath, initial_hash)
            save_hash(filepath, new_hash)

            loaded_hash = load_hash(filepath)
            self.assertEqual(new_hash, loaded_hash)
        finally:
            os.remove(filepath)
            
    def test_save_load_progress(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            progress_file = f.name
        try:
            progress = {
                'last_saved': 512,
                'total_size': 2048
            }
            save_progress(progress_file, progress)

            loaded_progress = load_progress(progress_file)
            self.assertEqual(progress, loaded_progress)
        finally:
            os.remove(progress_file)

    def test_load_progress_nonexistent_file(self):
        self.assertIsNone(load_progress("nonexistent_file.txt"))

    def test_save_with_checkpoints(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            filepath = f.name
        try:
            content = "Sample content for save_with_checkpoints function."
            checkpoint_size = 10

            saved_position = save_with_checkpoints(filepath, content, sample_processing_function, checkpoint_size)
            saved_content = read_file_mmap(filepath)

            self.assertEqual(len(content), saved_position)
            self.assertEqual(content.upper(), saved_content)  # Verify that the processing_function is applied
        finally:
            os.remove(filepath)

    def test_save_with_checkpoints_resume(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            filepath = f.name
        try:
            content = "Sample content for save_with_checkpoints function."
            checkpoint_size = 10

            initial_saved_position = save_with_checkpoints(filepath, content[:20], sample_processing_function, checkpoint_size=checkpoint_size)
            resume_saved_position = save_with_checkpoints(filepath, content, sample_processing_function, checkpoint_size=checkpoint_size)

            saved_content = read_file_mmap(filepath)

            self.assertEqual(len(content), resume_saved_position)
            self.assertEqual(content.upper(), saved_content)  # Verify that the processing_function is applied
        finally:
            os.remove(filepath)

            
    def test_process_chunk(self):
        def dummy_processing_function(data):
            return data.upper()

        manager = multiprocessing.Manager()
        output_dict = manager.dict()
        chunk = "This is a test chunk for process_chunk function."

        process_chunk(chunk, dummy_processing_function, output_dict)

        self.assertEqual(chunk.upper(), output_dict[chunk])

    def test_process_chunk_large(self):
        def dummy_processing_function(data):
            return data.upper()

        manager = multiprocessing.Manager()
        output_dict = manager.dict()

        # Generate a large chunk of random letters
        chunk = ''.join(random.choices(string.ascii_letters, k=50000))

        process_chunk(chunk, dummy_processing_function, output_dict)

        self.assertEqual(chunk.upper(), output_dict[chunk])

    def test_process_chunk_empty(self):
        def dummy_processing_function(data):
            return data.upper()

        manager = multiprocessing.Manager()
        output_dict = manager.dict()
        chunk = ""

        process_chunk(chunk, dummy_processing_function, output_dict)

        self.assertEqual(chunk.upper(), output_dict.get(chunk, ""))

    def test_process_chunk_custom_processing(self):
        def reverse_string(data):
            return data[::-1]

        manager = multiprocessing.Manager()
        output_dict = manager.dict()
        chunk = "This is a test chunk for process_chunk function."

        process_chunk(chunk, reverse_string, output_dict)

        self.assertEqual(chunk[::-1], output_dict[chunk])

    def test_resume_checkpoint(self):
        with tempfile.NamedTemporaryFile(delete=False) as input_f, \
                tempfile.NamedTemporaryFile(delete=False) as output_f, \
                tempfile.NamedTemporaryFile(delete=False) as hash_f, \
                tempfile.NamedTemporaryFile(delete=False) as progress_f:
            input_file = input_f.name
            output_file = output_f.name
            hash_file = hash_f.name
            progress_file = progress_f.name

            content = "Sample content for resume_checkpoint function."
            input_f.write(content.encode())
            input_f.flush()

            checkpoint_size = 10
            saved_position = 0

            # Simulate an interruption after processing 20 characters
            with mock.patch('file_processing.save_with_checkpoints') as mock_save_with_checkpoints:
                mock_save_with_checkpoints.side_effect = lambda *args, **kwargs: min(saved_position + checkpoint_size, len(content))
                saved_position = resume_checkpoint(input_file, output_file, hash_file, progress_file, sample_processing_function)

            # Call resume_checkpoint again to verify that it resumes correctly
            saved_position = resume_checkpoint(input_file, output_file, hash_file, progress_file, sample_processing_function)

            saved_content = read_file_mmap(output_file)

            self.assertEqual(len(content), saved_position)
            self.assertEqual(content.upper(), saved_content)

            os.remove(input_file)
            os.remove(output_file)
            os.remove(hash_file)
            os.remove(progress_file)

if __name__ == '__main__':
    unittest.main()
