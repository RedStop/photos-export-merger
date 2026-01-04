import os
import json
import re
from pathlib import Path
from typing import Tuple, Optional, Set, Dict, Any

BRACKET_PATTERN = re.compile(r'\((\d+)\)\.json$')
FILE_PATTERN = re.compile(r'^(.+?)\((\d+)\)(\.[^.]+)$')

def JsonFileFinder(
    json_path: str,
    json_data: Optional[Dict[str, Any]] = None,
    dir_files: Optional[Set[str]] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Find a file matching a .json file and read its new name from the 'title' field.
    
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
        
        # Get the directory and json filename
        directory = json_path_obj.parent
        json_filename = json_path_obj.name
        
        # Strategy 1: Try exact match (remove .json extension)
        if json_filename.endswith('.json'):
            base_name = json_filename[:-5]  # Remove '.json'
            
            # If dir_files provided, check in the set; otherwise check filesystem
            if dir_files is not None:
                if base_name in dir_files:
                    return (base_name, new_title)
            else:
                candidate_path = directory / base_name
                if candidate_path.exists() and candidate_path.is_file():
                    return (base_name, new_title)
        
        # Strategy 2: Handle bracket notation and truncated filenames
        # Pattern: filename(N).json or truncated versions
        bracket_match = BRACKET_PATTERN.search(json_filename)
        
        if bracket_match:
            # Has bracket notation like (2).json
            bracket_num = bracket_match.group(1)
            # Remove (N).json to get base
            base_without_bracket = json_filename[:bracket_match.start()]
            
            # Get filenames to check
            if dir_files is not None:
                # dir_files already excludes JSON files, use directly as set
                filenames_to_check = dir_files
            else:
                try:
                    files_in_dir = list(directory.iterdir())
                    filenames_to_check = {f.name for f in files_in_dir if f.is_file() and not f.name.endswith('.json')}
                except OSError:
                    return (None, new_title)
            
            for filename in filenames_to_check:
                # Check if file matches pattern: base*(N).ext
                file_pattern = FILE_PATTERN.search(filename)
                if file_pattern:
                    file_base = file_pattern.group(1)
                    file_bracket = file_pattern.group(2)
                    
                    # Check if bracket number matches and base is a prefix match
                    if file_bracket == bracket_num and file_base.startswith(base_without_bracket[:len(file_base)]):
                        return (filename, new_title)
        
        # Strategy 3: Handle truncated filenames without brackets
        # Remove .json and try to find files with matching prefix
        if json_filename.endswith('.json'):
            base_without_json = json_filename[:-5]
            
            # Get filenames to check
            if dir_files is not None:
                # dir_files already excludes JSON files, use directly as set
                filenames_to_check = dir_files
            else:
                try:
                    files_in_dir = list(directory.iterdir())
                    filenames_to_check = {f.name for f in files_in_dir if f.is_file() and not f.name.endswith('.json')}
                except OSError:
                    return (None, new_title)
            
            for filename in filenames_to_check:
                # Check if removing the file's extension gives us something that starts with our base
                # The json base should be a prefix of: filestem + possible partial extension
                full_name_parts = filename.rsplit('.', 1)
                if len(full_name_parts) == 2:
                    stem, ext = full_name_parts
                    # Check all possible truncations
                    for i in range(len(ext) + 1):
                        potential_truncated = stem + ('.' + ext[:i] if i > 0 else '')
                        if potential_truncated == base_without_json:
                            return (filename, new_title)
        
        # No matching file found
        return (None, new_title)
        
    except Exception:
        # Catch any unexpected errors
        return (None, None)