import pandas as pd

# File paths
blocklist_file = r"C:\Users\dabra\Documents\blocklist\Blocklist.csv"
import_blocklist_file = r"C:\Users\dabra\Documents\blocklist\blackList_exportado.csv"
output_file = r"C:\Users\dabra\Documents\blocklist\import_block_final.csv"

# Step 1: Read the numbers from the import list into a set for fast lookups.
# This is much more memory-efficient than loading the whole file into a pandas DataFrame.
try:
    with open(import_blocklist_file, 'r', encoding='utf-8') as f:
        # Create a set of phone numbers from the first column of the file
        phones_to_exclude = {line.strip().split(';', 1)[0] for line in f if line.strip()}
except FileNotFoundError:
    print(f"Warning: {import_blocklist_file} not found. Proceeding without an exclusion list.")
    phones_to_exclude = set()

# Step 2: Process the large blocklist file in chunks to avoid high memory usage.
chunksize = 1_000_000 # Process 1 million rows at a time; adjust based on available RAM
is_first_chunk = True

print("Processing blocklist file...")
try:
    # Read the large CSV in chunks
    for chunk in pd.read_csv(blocklist_file, sep=",", dtype=str, chunksize=chunksize):
        # Filter the chunk to keep only rows where 'TELEFONE' is not in the exclusion set
        filtered_df = chunk[~chunk['TELEFONE'].isin(phones_to_exclude)]

        # If the filtered chunk is not empty, write it to the output file
        if not filtered_df.empty:
            if is_first_chunk:
                # For the first time, write with header and overwrite existing file
                filtered_df[['TELEFONE']].to_csv(output_file, index=False, mode='w', encoding='utf-8')
                is_first_chunk = False
            else:
                # For subsequent writes, append without header
                filtered_df[['TELEFONE']].to_csv(output_file, index=False, mode='a', header=False, encoding='utf-8')

    print(f"Finished processing. Output saved to {output_file}")

except FileNotFoundError:
    print(f"Error: {blocklist_file} not found. Please check the file path.")
except Exception as e:
    print(f"An error occurred: {e}")