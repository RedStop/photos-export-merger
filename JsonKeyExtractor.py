import json
import sys
from pathlib import Path
from collections import defaultdict
from JsonFileIdentifier import JsonFileFinder

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
    missingFiles = []
    titlesByFolder = {}  # Dictionary to track titles by folder
    duplicateTitles = []  # List of duplicate titles within same folder
    
    # Track all file types
    fileTypeTracking = defaultdict(list)
    
    # Find all files recursively (not just JSON)
    allFiles = [f for f in directory.rglob('*') if f.is_file()]
    
    # Categorize files by extension
    for file in allFiles:
        ext = file.suffix.lower()  # Get extension in lowercase
        if not ext:
            ext = 'no_extension'
        else:
            ext = ext[1:]  # Remove the leading dot
        
        relativePath = str(file.relative_to(directory))
        fileTypeTracking[ext].append(relativePath)
    
    # Find all JSON files for processing
    jsonFiles = list(directory.rglob('*.json'))
    
    if not jsonFiles:
        print(f"No JSON files found in '{directoryPath}'")
        return
    
    print(f"Found {len(jsonFiles)} JSON file(s) to process...")
    print(f"Found {len(allFiles)} total file(s) in the directory tree")
    
    for jsonFile in jsonFiles:
        try:
            data = None
            with open(jsonFile, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if data is not None:
                # Store with relative path as key
                relativePath = jsonFile.relative_to(directory)
                matchingFilename, newTitle = JsonFileFinder(jsonFile)

                if newTitle is None:
                    raise Exception(f"Title for {relativePath} is not available.")
                
                if matchingFilename is None:
                    missingFiles.append({
                        "json_file": str(relativePath)
                    })
                
                # Get the key structure (first and second level)
                structure = getNestedKeys(data, maxDepth=2)
                # Merge into combined structure
                combinedStructure = mergeStructures(combinedStructure, structure, "", str(relativePath), typeConflicts)
                # Add the linked file to the structure
                structure["MatchingFile"] = matchingFilename
                allStructures[str(relativePath)] = structure
                
                # Track titles by folder to detect duplicates within the same folder
                folderPath = str(jsonFile.parent.relative_to(directory))
                if folderPath not in titlesByFolder:
                    titlesByFolder[folderPath] = {}
                if newTitle not in titlesByFolder[folderPath]:
                    titlesByFolder[folderPath][newTitle] = []
                titlesByFolder[folderPath][newTitle].append(str(relativePath))

                filesProcessed += 1
                
        except json.JSONDecodeError as e:
            filesWithErrors.append((str(jsonFile), f"JSON decode error: {e}"))
        except Exception as e:
            filesWithErrors.append((str(jsonFile), f"Error: {e}"))

    # Find duplicate titles within each folder
    for folderPath, titles in titlesByFolder.items():
        for title, jsonFilesList in titles.items():
            if len(jsonFilesList) > 1:
                duplicateTitles.append({
                    "folder": folderPath,
                    "title": title,
                    "json_files": jsonFilesList
                })

    # Prepare file type summary
    # Extensions to show only count (not individual files)
    countOnlyExtensions = {'json', 'jpg', 'jpeg', 'mp4'}
    
    fileTypeSummary = {
        "summary": {},
        "detailed_listings": {}
    }
    
    for ext, files in sorted(fileTypeTracking.items()):
        fileTypeSummary["summary"][ext] = len(files)
        
        # Only add detailed listing if not in count-only list
        if ext.lower() not in countOnlyExtensions:
            fileTypeSummary["detailed_listings"][ext] = sorted(files)
    
    # Create final output with both individual and combined structures
    outputData = {
        "combined_structure": combinedStructure,
        "individual_files": allStructures,
        "file_types": fileTypeSummary
    }
    
    # Add type conflicts if any were found
    if typeConflicts:
        outputData["type_conflicts"] = typeConflicts
    
    # Add duplicate titles if any were found
    if duplicateTitles:
        outputData["duplicate_titles"] = duplicateTitles

    # Add missing files if any were found
    if missingFiles:
        outputData["missing_files"] = missingFiles
    
    # Save the results
    outputPath = Path(outputFile)
    with open(outputPath, 'w', encoding='utf-8') as f:
        json.dump(outputData, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print(f"\nProcessing complete!")
    print(f"Files processed successfully: {filesProcessed}")
    print(f"Files with errors: {len(filesWithErrors)}")
    
    if filesWithErrors:
        print("Errors encountered:")
        for filePath, error in filesWithErrors:
            print(f"  - {filePath}: {error}")
    
    print(f"File types found:")
    for ext, count in sorted(fileTypeSummary["summary"].items(), key=lambda x: x[1], reverse=True):
        print(f"  - {ext}: {count}")
    
    if typeConflicts:
        print(f"Type conflicts found: {len(typeConflicts)}")
    
    if duplicateTitles:
        totalDuplicates = sum(len(dup['json_files']) for dup in duplicateTitles)
        print(f"Duplicate titles found: {len(duplicateTitles)} title(s) with duplicates in the same folder")
        print(f" - Total JSON files with duplicate titles: {totalDuplicates}")
    
    if missingFiles:
        print(f"Missing files: {len(missingFiles)}")
    
    print(f"\nOutput saved to: {outputPath.absolute()}")
    print(f"The output contains:")
    print(f"  - 'combined_structure': Merged structure from all files")
    print(f"  - 'individual_files': Structure for each file")
    print(f"  - 'file_types': Summary and detailed listings of all file types")
    print(f"    - 'summary': Count of each file type")
    print(f"    - 'detailed_listings': Individual files (except json, jpg, jpeg, mp4)")
    if typeConflicts:
        print(f"  - 'type_conflicts': List of type mismatches found")
    if duplicateTitles:
        print(f"  - 'duplicate_titles': JSON files with duplicate title fields in same folder")
    if missingFiles:
        print(f"  - 'missing_files': List of files described by JSON but not found on disk")

if __name__ == "__main__":
    # Specify the directory to scan
    directoryToScan = "."  # Current directory, use arg to change
    if len(sys.argv) > 1:
        directoryToScan = sys.argv[1]
    
    # Optional: specify custom output file name
    outputFilename = "extracted_keys.json"
    
    processJsonFiles(directoryToScan, outputFilename)

    # TODO:
    # Detect photo's that does not have a .json file.
    # Find the missing .json files.
    # Ensure each .json file points to a unique photo