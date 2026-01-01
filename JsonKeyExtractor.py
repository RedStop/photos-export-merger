import json
import sys
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

def mergeStructures(struct1, struct2):
    """
    Merge two structure dictionaries, combining all keys from both.
    """
    if not isinstance(struct1, dict) or not isinstance(struct2, dict):
        # If either is not a dict, prefer dict over other types, otherwise return struct2
        if isinstance(struct1, dict):
            return struct1
        elif isinstance(struct2, dict):
            return struct2
        else:
            return struct2
    
    result = struct1.copy()
    
    for key, value in struct2.items():
        if key in result:
            # If both have the key, merge their values recursively
            if isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = mergeStructures(result[key], value)
            elif isinstance(result[key], list) and isinstance(value, list):
                # For lists, merge the structure of their first elements
                if result[key] and value:
                    result[key] = [mergeStructures(result[key][0], value[0])]
                elif value:
                    result[key] = value
            # Otherwise, keep existing value (could also choose to keep the new one)
        else:
            # Key only exists in struct2, add it
            result[key] = value
    
    return result

def processJsonFiles(directory_path, output_file='extracted_keys.json'):
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
    combined_structure = {}
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
                
                # Merge into combined structure
                combined_structure = mergeStructures(combined_structure, structure)
                
                files_processed += 1
                
        except json.JSONDecodeError as e:
            files_with_errors.append((str(json_file), f"JSON decode error: {e}"))
        except Exception as e:
            files_with_errors.append((str(json_file), f"Error: {e}"))
    
    # Create final output with both individual and combined structures
    output_data = {
        "combined_structure": combined_structure,
        "individual_files": all_structures
    }
    
    # Save the results
    output_path = Path(output_file)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print(f"\nProcessing complete!")
    print(f"Files processed successfully: {files_processed}")
    print(f"Files with errors: {len(files_with_errors)}")
    
    if files_with_errors:
        print("\nErrors encountered:")
        for file_path, error in files_with_errors:
            print(f"  - {file_path}: {error}")
    
    print(f"\nOutput saved to: {output_path.absolute()}")
    print(f"\nThe output contains:")
    print(f"  - 'combined_structure': Merged structure from all files")
    print(f"  - 'individual_files': Structure for each file")

if __name__ == "__main__":
    # Specify the directory to scan
    directory_to_scan = "."  # Current directory, use arg to change
    if len(sys.argv) > 1:
        directory_to_scan = sys.argv[1]
    
    # Optional: specify custom output file name
    output_filename = "extracted_keys.json"
    
    processJsonFiles(directory_to_scan, output_filename)