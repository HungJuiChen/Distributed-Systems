import unittest
import subprocess
import os
from MP_1.log_gen import FREQUENT_PATTERN, INFREQUENT_PATTERN, RARE_PATTERN

CORRECT_FREQ = [0, 0, 0, 0, 0, 100000, 100000, 100000, 100000, 100000]
CORRECT_INFREQ = [0, 0, 0, 0, 0, 10000, 10000, 10000, 10000, 10000]
CORRECT_RARE = [0, 0, 0, 0, 0, 1000, 1000, 1000, 1000, 1000]

class TestGrepQueryClient(unittest.TestCase):

    def run_client_program(self, pattern):
        """Helper function to run the client program with given grep pattern and flags."""
        # Construct the command for running the client program
        command = ['python3', 'client.py', '--', '-H']  
        
        command.append(pattern)

        # For some reason subprocess.run generates a file called unittest.[test module name].log
        # but neither the client or server support this behavior. This is a bandaid solution to 
        # remove the unwanted file but any feedback on why this is happening is appreciated.
        # This problem ONLY appears in the unit test.
        # ==============================================================================
        result = subprocess.run(command, capture_output=True, text=True)
        
        test_module_name = os.path.splitext(os.path.basename(__file__))[0]

        log_file = f'unittest.{test_module_name}.log'

        # Remove the log file if it exists
        if os.path.exists(log_file):
            os.remove(log_file)
        # ===============================================================================

        return result.stdout, result.stderr

    def run_grep(self, pattern, log_file):
        """Helper function to run grep on the specified log file."""
        command = ['grep', '-H', pattern, log_file]
        result = subprocess.run(command, capture_output=True, text=True)
        
        return result.stdout

    def compare_with_expected_counts(self, stdout, expected_counts):
        """Helper function to compare stdout with a list of expected counts."""
        lines = stdout.splitlines()
        lines.pop()

        # Convert lines to integers
        actual_counts = [int(l.split(':')[1]) for l in lines]

        # Ensure the number of counts match
        self.assertEqual(len(actual_counts), len(expected_counts), 
                         f"Expected {len(expected_counts)} counts, but got {len(actual_counts)}")

        # Compare actual counts with expected counts
        for actual, expected in zip(actual_counts, expected_counts):
            self.assertEqual(actual, expected, f"Expected count {expected} but got {actual}")

    def compare_logs_with_grep(self, output_dir, pattern):
        """Helper function to compare output log files with the result of a grep command."""
        for log_file in os.listdir(os.curdir):
            if log_file.startswith("unittest.") and log_file.endswith(".log"):

                output_log = log_file.replace("unittest", "output")
                output_log_path = os.path.join(output_dir, output_log)

                self.assertTrue(os.path.exists(output_log_path), f"Missing {output_log_path}, log_file: {log_file}")

                grep_output = self.run_grep(pattern, log_file)

                with open(output_log_path, 'r') as output_file:
                    output_content = output_file.read()

                # Compare the grep output with the content of the output log file
                self.assertEqual(grep_output.strip(), output_content.strip(), 
                                 f"{output_log_path} does not match grep output from {log_file}")

    def test_frequent_pattern(self):
        """Test for a frequent pattern that should appear many times."""
        pattern = FREQUENT_PATTERN  
        expected_counts = CORRECT_FREQ
        
        stdout, stderr = self.run_client_program(pattern)

        self.assertEqual(stderr, '')

        self.compare_with_expected_counts(stdout, expected_counts)

        self.compare_logs_with_grep('output', pattern) 

    def test_rare_pattern(self):
        """Test for a rare pattern that should appear once."""
        pattern = RARE_PATTERN  
        expected_counts = CORRECT_RARE 
        
        stdout, stderr = self.run_client_program(pattern)

        self.assertEqual(stderr, '')

        self.compare_with_expected_counts(stdout, expected_counts)

        self.compare_logs_with_grep('output', pattern)

    def test_somewhat_frequent_pattern(self):
        """Test for a somewhat frequent pattern."""
        pattern = INFREQUENT_PATTERN  
        expected_counts = CORRECT_INFREQ 
        
        stdout, stderr = self.run_client_program(pattern)

        self.assertEqual(stderr, '')

        self.compare_with_expected_counts(stdout, expected_counts)

        self.compare_logs_with_grep('output', pattern)

if __name__ == '__main__':
    unittest.main()
