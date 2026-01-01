import json
import sys
from pathlib import Path
from collections import defaultdict

def getNestedKeys(data, maxDepth=2, currentDepth=0):
    """
    Extract keys up to a specified depth from a JSON structure.
    Returns a dictionary with the structure of keys.
    """
    if currentDepth >= maxDepth:
        return None
    
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if currentDepth < maxDepth - 1:
                nested = getNestedKeys(value, maxDepth, currentDepth + 1)
                result[key] = nested if nested else type(value).__name__
            else:
                result[key] = type(value).__name__
        return result
    elif isinstance(data, list) and data:
        # For lists, analyze the first item to understand structure
        return [getNestedKeys(data[0], maxDepth, currentDepth)]
    else:
        return type(data).__name__

def mergeStructures(struct1, struct2, currentPath="", currentFile="", typeConflicts=None):
    """
    Merge two structure dictionaries, combining all keys from both.
    Detects type conflicts and records them.
    """
    if typeConflicts is None:
        typeConflicts = []
    
    if not isinstance(struct1, dict) or not isinstance(struct2, dict):
        # Type mismatch at this level
        if isinstance(struct1, dict) and not isinstance(struct2, dict):
            typeConflicts.append({
                "path": currentPath,
                "file": currentFile,
                "expected_type": "dict",
                "found_type": struct2 if isinstance(struct2, str) else type(struct2).__name__
            })
            return struct1
        elif isinstance(struct2, dict) and not isinstance(struct1, dict):
            typeConflicts.append({
                "path": currentPath,
                "file": currentFile,
                "expected_type": "dict",
                "found_type": struct1 if isinstance(struct1, str) else type(struct1).__name__
            })
            return struct2
        else:
            # Both are non-dict types, check if they match
            type1 = struct1 if isinstance(struct1, str) else type(struct1).__name__
            type2 = struct2 if isinstance(struct2, str) else type(struct2).__name__
            if type1 != type2:
                typeConflicts.append({
                    "path": currentPath,
                    "file": currentFile,
                    "expected_type": type1,
                    "found_type": type2
                })
            return struct2
    
    result = struct1.copy()
    
    for key, value in struct2.items():
        newPath = f"{currentPath}.{key}" if currentPath else key
        
        if key in result:
            # If both have the key, merge their values recursively
            if isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = mergeStructures(result[key], value, newPath, currentFile, typeConflicts)
            elif isinstance(result[key], list) and isinstance(value, list):
                # For lists, merge the structure of their first elements
                if result[key] and value:
                    result[key] = [mergeStructures(result[key][0], value[0], f"{newPath}[0]", currentFile, typeConflicts)]
                elif value:
                    result[key] = value
            else:
                # Type mismatch for this key
                type1 = result[key] if isinstance(result[key], str) else ("list" if isinstance(result[key], list) else "dict")
                type2 = value if isinstance(value, str) else ("list" if isinstance(value, list) else "dict")
                if type1 != type2:
                    typeConflicts.append({
                        "path": newPath,
                        "file": currentFile,
                        "expected_type": type1,
                        "found_type": type2
                    })
        else:
            # Key only exists in struct2, add it
            result[key] = value
    
    return result

def processJsonFiles(directoryPath, outputFile='extracted_keys.json'):
    """
    Process all JSON files in directory and subdirectories.
    Extract first and second level keys and save to a new JSON file.
    """
    directory = Path(directoryPath)
    
    if not directory.exists():
        print(f"Error: Directory '{directoryPath}' does not exist")
        return
    
    # Dictionary to store all unique key structures found
    allStructures = {}
    combinedStructure = {}
    typeConflicts = []
    filesProcessed = 0
    filesWithErrors = []
    mkvFiles = []
    
    # Find all JSON files recursively
    jsonFiles = list(directory.rglob('*.json'))
    
    # Find all MKV files recursively
    mkvFiles = list(directory.rglob('*.mkv'))
    
    if not jsonFiles:
        print(f"No JSON files found in '{directoryPath}'")
        return
    
    print(f"Found {len(jsonFiles)} JSON file(s) to process...")
    print(f"Found {len(mkvFiles)} MKV file(s) in the directory tree")
    
    for jsonFile in jsonFiles:
        try:
            with open(jsonFile, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Get the key structure (first and second level)
                structure = getNestedKeys(data, maxDepth=2)
                
                # Store with relative path as key
                relativePath = jsonFile.relative_to(directory)
                allStructures[str(relativePath)] = structure
                
                # Merge into combined structure
                combinedStructure = mergeStructures(combinedStructure, structure, "", str(relativePath), typeConflicts)
                
                filesProcessed += 1
                
        except json.JSONDecodeError as e:
            filesWithErrors.append((str(jsonFile), f"JSON decode error: {e}"))
        except Exception as e:
            filesWithErrors.append((str(jsonFile), f"Error: {e}"))
    
    # Create final output with both individual and combined structures
    outputData = {
        "combined_structure": combinedStructure,
        "individual_files": allStructures,
        "mkv_files": [str(mkv.relative_to(directory)) for mkv in mkvFiles]
    }
    
    # Add type conflicts if any were found
    if typeConflicts:
        outputData["type_conflicts"] = typeConflicts
    
    # Save the results
    outputPath = Path(outputFile)
    with open(outputPath, 'w', encoding='utf-8') as f:
        json.dump(outputData, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print(f"\nProcessing complete!")
    print(f"Files processed successfully: {filesProcessed}")
    print(f"Files with errors: {len(filesWithErrors)}")
    
    if filesWithErrors:
        print("\nErrors encountered:")
        for filePath, error in filesWithErrors:
            print(f"  - {filePath}: {error}")
    
    if typeConflicts:
        print(f"\nType conflicts found: {len(typeConflicts)}")
        print("\nType conflicts:")
        for conflict in typeConflicts:
            print(f"  - Path: {conflict['path']}")
            print(f"    File: {conflict['file']}")
            print(f"    Expected type: {conflict['expected_type']}")
            print(f"    Found type: {conflict['found_type']}")
            print()
    
    if mkvFiles:
        print(f"\nMKV files found: {len(mkvFiles)}")
        print("\nMKV files:")
        for mkvFile in mkvFiles:
            print(f"  - {mkvFile.relative_to(directory)}")
    
    print(f"\nOutput saved to: {outputPath.absolute()}")
    print(f"\nThe output contains:")
    print(f"  - 'combined_structure': Merged structure from all files")
    print(f"  - 'individual_files': Structure for each file")
    print(f"  - 'mkv_files': List of all MKV files found")
    if typeConflicts:
        print(f"  - 'type_conflicts': List of type mismatches found")

if __name__ == "__main__":
    # Specify the directory to scan
    directoryToScan = "."  # Current directory, use arg to change
    if len(sys.argv) > 1:
        directoryToScan = sys.argv[1]
    
    # Optional: specify custom output file name
    outputFilename = "extracted_keys.json"
    
    processJsonFiles(directoryToScan, outputFilename)