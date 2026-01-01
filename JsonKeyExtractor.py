import json
import os
from pathlib import Path
from collections import defaultdict

def getNestedKeys(data, max_depth=2, current_depth=0):
    """
    Extract keys up to a specified depth from a JSON structure.
    Returns a dictionary with the structure of keys.
    """
    if current_depth >= max_depth:
        return None
    
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if current_depth < max_depth - 1:
                nested = getNestedKeys(value, max_depth, current_depth + 1)
                result[key] = nested if nested else type(value).__name__
            else:
                result[key] = type(value).__name__
        return result
    elif isinstance(data, list) and data:
        # For lists, analyze the first item to understand structure
        return [getNestedKeys(data[0], max_depth, current_depth)]
    else:
        return type(data).__name__

def processJsonFiles(directory_path, output_file='output_keys.json'):
    """
    Process all JSON files in directory and subdirectories.
    Extract first and second level keys and save to a new JSON file.
    """
    directory = Path(directory_path)
    
    if not directory.exists():
        print(f"Error: Directory '{directory_path}' does not exist")
        return
    
    # Dictionary to store all unique key structures found
    all_structures = {}
    files_processed = 0
    files_with_errors = []
    
    # Find all JSON files recursively
    json_files = list(directory.rglob('*.json'))
    
    if not json_files:
        print(f"No JSON files found in '{directory_path}'")
        return
    
    print(f"Found {len(json_files)} JSON file(s) to process...")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Get the key structure (first and second level)
                structure = getNestedKeys(data, max_depth=2)
                
                # Store with relative path as key
                relative_path = json_file.relative_to(directory)
                all_structures[str(relative_path)] = structure
                
                files_processed += 1
                
        except json.JSONDecodeError as e:
            files_with_errors.append((str(json_file), f"JSON decode error: {e}"))
        except Exception as e:
            files_with_errors.append((str(json_file), f"Error: {e}"))
    
    # Save the results
    output_path = Path(output_file)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_structures, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print(f"\nProcessing complete!")
    print(f"Files processed successfully: {files_processed}")
    print(f"Files with errors: {len(files_with_errors)}")
    
    if files_with_errors:
        print("\nErrors encountered:")
        for file_path, error in files_with_errors:
            print(f"  - {file_path}: {error}")
    
    print(f"\nOutput saved to: {output_path.absolute()}")

if __name__ == "__main__":
    # Specify the directory to scan
    directory_to_scan = "."  # Current directory, change as needed
    
    # Optional: specify custom output file name
    output_filename = "extracted_keys.json"
    
    processJsonFiles(directory_to_scan, output_filename)
