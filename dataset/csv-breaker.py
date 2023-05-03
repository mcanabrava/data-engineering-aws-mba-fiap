import os
import pandas as pd
import threading

def process_chunk(chunk, output_file):
    # Write the chunk to a separate CSV file
    chunk.to_csv(output_file, index=False)

    print(f"Chunk saved to {output_file}")

def split_large_csv(input_file, chunk_size, max_files):
    # Use the current directory as the output directory
    output_dir = os.getcwd()

    print("Starting the splitting process...")

    try:
        # Get the total number of rows in the CSV file
        total_rows = sum(1 for _ in open(input_file, 'r', encoding='utf-8-sig'))

        # Calculate the number of chunks based on the chunk size
        num_chunks = (total_rows - 1) // chunk_size + 1

        # Create a list to store the threads
        threads = []

        # Read the large CSV file in chunks
        for i in range(num_chunks):
            skip_rows = i * chunk_size

            # Generate the output file path
            output_file = os.path.join(output_dir, f"output_{i}.csv")

            print(f"Processing chunk {i}...")

            # Read only the required chunk of the CSV file
            chunk = pd.read_csv(input_file, skiprows=skip_rows, nrows=chunk_size, encoding='utf-8-sig')

            # Create a thread to process the chunk
            thread = threading.Thread(target=process_chunk, args=(chunk, output_file))
            thread.start()

            # Add the thread to the list
            threads.append(thread)

            # Check if the maximum number of files has been reached
            if i + 1 >= max_files:
                break

        # Wait for all the threads to complete
        for thread in threads:
            thread.join()

        print("Splitting completed.")

    except pd.errors.ParserError as e:
        print(f"Error parsing the CSV file: {e}")

# Usage example
input_file = "library-collection-inventory.csv"
chunk_size = 100000  # 1 million rows per chunk
max_files = 10  # Maximum number of files to generate

split_large_csv(input_file, chunk_size, max_files)