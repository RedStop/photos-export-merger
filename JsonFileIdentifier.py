from pathlib import Path
from sortedcontainers import SortedSet
from typing import Tuple, Optional, List, Dict, Any
import json
import os
import re

BRACKET_PATTERN = re.compile(r'\((\d+)\)$')

def starts_with(words:SortedSet, prefix:str):
    '''
    Find all words that start with given prefix.
    O(log n + k) where k is the number of matching words.
    '''
    # Find the starting index using bisect_left
    start_idx = words.bisect_left(prefix)
    
    # Collect words while they match prefix
    result = []
    for i in range(start_idx, len(words)):
        word = str(words[i])
        if word.startswith(prefix):
            result.append(word)
        else:
            break  # Stop when we hit a non-matching word
    return result

def insert_before_ext(original_string, string_to_insert):
    """
    Inserts a string immediately before the last dot in a given string.

    Args:
        original_string: The string to modify.
        string_to_insert: The string to insert.

    Returns:
        The modified string, or the original string if no dot is found.
    """
    last_dot_index = original_string.rfind('.')
    
    if last_dot_index == -1:
        # No dot found, return original string or handle as needed
        return original_string
    else:
        # Slice the string into two parts and concatenate with the new string
        part_before_dot = original_string[:last_dot_index]
        part_after_dot = original_string[last_dot_index:]
        
        new_string = part_before_dot + string_to_insert + part_after_dot
        return new_string

def JsonFileFinder(
    json_path: str,
    json_data: Optional[Dict[str, Any]] = None,
    dir_files: Optional[SortedSet[str]] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Find a file matching a .json file and read its new name from the 'title' field.
    It is assumed that the new name in the 'title' field will have the same extention (case insensitive),
    than the file trying to be matched. If this is not the case, then this function might not find the
    matching file.
    
    Args:
        json_path: Path to the .json file
        json_data: Optional pre-loaded JSON data (to avoid re-parsing)
        dir_files: Optional set of non-JSON filenames (not full paths) in the same directory
        
    Returns:
        Tuple of (matching_filename, new_title):
        - matching_filename: The filename (not full path) of the matching file, or None if not found
        - new_title: The title read from the json file, or None if error reading json
    """
    try:
        json_path_obj = Path(json_path)
        
        # If json_data is provided, we assume the file exists and use the data
        # Otherwise, check if file exists and read it
        if json_data is None:
            if not json_path_obj.exists() or not json_path_obj.is_file():
                return (None, None)
            
            try:
                with open(json_path_obj, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
            except (json.JSONDecodeError, IOError, KeyError, UnicodeDecodeError):
                return (None, None)
        
        # Extract title from json data
        new_title = json_data.get('title')
        if new_title is None:
            return (None, None)
        
        # Get the json filename
        json_filename = json_path_obj.name
        
        # Ensure that the json_data ends with .json
        if not json_filename.endswith('.json'):
            return (None, new_title)
        
        # Get the filename without the .json and the directory of the .json file
        json_base_filename = json_filename[:-5]  # Remove '.json'
        directory = json_path_obj.parent

        # The expected filename of the matching file
        expected_matching_filename = json_base_filename
        new_title_base, new_title_ext = os.path.splitext(new_title)
        new_title_ext = str(new_title_ext)
        new_title_ext_lower = new_title_ext.lower()
        
        # Check if the filename ended with "(x)"
        json_base_without_bracket = json_base_filename
        bracket_num = None
        tryExactMatch = True
        if json_base_filename.endswith(")"):
            bracket_match = BRACKET_PATTERN.search(json_base_filename)
            if bracket_match:
                # Has bracket notation like (2).json
                bracket_num = bracket_match.group(1)
                # Remove (N).json to get base
                json_base_without_bracket = json_base_filename[:bracket_match.start()]

                # If there is a full extension, that matches the extention of the bracket
                # can be safely added for a quick lookup of the name
                if json_base_without_bracket.lower().endswith(new_title_ext_lower):
                    expected_matching_filename = insert_before_ext(json_base_without_bracket, f"({bracket_num})")
                else:
                    tryExactMatch = False

        if tryExactMatch:
            # Try exact match (remove .json extension), if there is no "(x)"
            # If dir_files provided, check in the set; otherwise check filesystem
            if dir_files is not None:
                if expected_matching_filename in dir_files:
                    return (expected_matching_filename, new_title)
            else:
                candidate_path = directory / expected_matching_filename
                if candidate_path.exists() and candidate_path.is_file():
                    return (expected_matching_filename, new_title)

        # More complex lookup must be done, since exact match could not match a file
        # Get filenames to check
        filenames_to_check:SortedSet = None
        if dir_files is not None:
            # dir_files already excludes JSON files, use directly as set
            filenames_to_check = dir_files
        else:
            try:
                files_in_dir = list(directory.iterdir())
                filenames_to_check = SortedSet({f.name for f in files_in_dir if f.is_file() and not f.name.endswith('.json')})
            except OSError:
                return (None, new_title)

        # Find all filenames that starts with the base part of the .json file
        shortlisted_filenames_to_check:List[str] = starts_with(filenames_to_check, json_base_without_bracket)
        # Only look at files with the correct file type
        shortlisted_filenames_to_check = [f for f in shortlisted_filenames_to_check if f.lower().endswith(new_title_ext_lower)]
        if len(shortlisted_filenames_to_check) == 1:
            # If there is only one match, use it
            return (shortlisted_filenames_to_check[0], new_title)
        elif len(shortlisted_filenames_to_check) > 1:
            # Take the "(x)" into account
            for file in shortlisted_filenames_to_check:
                ext_len = len(new_title_ext_lower)
                file_no_ext = file[:-ext_len] if ext_len else file
                file_bracket_match = BRACKET_PATTERN.search(file_no_ext)
                file_bracket_num = None if file_bracket_match is None else file_bracket_match.group(1)
                if file_bracket_num == bracket_num:
                    return (file, new_title)

        # No matching file found
        return (None, new_title)
    
    except Exception as e:
        print("Exception:", e)
        # Catch any unexpected errors
        return (None, None)