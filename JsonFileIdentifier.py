import os
import json
import re
from pathlib import Path
from typing import Tuple, Optional


def JsonFileFinder(json_path: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Find a file matching a .json file and read its new name from the 'title' field.
    
    Args:
        json_path: Path to the .json file
        
    Returns:
        Tuple of (matching_filename, new_title):
        - matching_filename: The filename (not full path) of the matching file, or None if not found
        - new_title: The title read from the json file, or None if error reading json
    """
    try:
        json_path_obj = Path(json_path)
        
        # Check if json file exists
        if not json_path_obj.exists() or not json_path_obj.is_file():
            return (None, None)
        
        # Read the title from json file
        try:
            with open(json_path_obj, 'r', encoding='utf-8') as f:
                data = json.load(f)
                new_title = data.get('title')
                if new_title is None:
                    return (None, None)
        except (json.JSONDecodeError, IOError, KeyError, UnicodeDecodeError):
            return (None, None)
        
        # Get the directory and json filename
        directory = json_path_obj.parent
        json_filename = json_path_obj.name
        
        # Strategy 1: Try exact match (remove .json extension)
        if json_filename.endswith('.json'):
            base_name = json_filename[:-5]  # Remove '.json'
            candidate_path = directory / base_name
            if candidate_path.exists() and candidate_path.is_file():
                return (base_name, new_title)
        
        # Strategy 2: Handle bracket notation and truncated filenames
        # Pattern: filename(N).json or truncated versions
        bracket_match = re.search(r'\((\d+)\)\.json$', json_filename)
        
        if bracket_match:
            # Has bracket notation like (2).json
            bracket_num = bracket_match.group(1)
            # Remove (N).json to get base
            base_without_bracket = json_filename[:bracket_match.start()]
            
            # The base might be truncated, we need to find files that:
            # 1. Start with the truncated base
            # 2. Have (N) before their extension
            try:
                files_in_dir = list(directory.iterdir())
            except OSError:
                return (None, new_title)
            
            for file in files_in_dir:
                if file.is_file() and file.name != json_filename:
                    # Check if file matches pattern: base*(N).ext
                    file_pattern = re.search(r'^(.+?)\((\d+)\)(\.[^.]+)$', file.name)
                    if file_pattern:
                        file_base = file_pattern.group(1)
                        file_bracket = file_pattern.group(2)
                        
                        # Check if bracket number matches and base is a prefix match
                        if file_bracket == bracket_num and file_base.startswith(base_without_bracket[:len(file_base)]):
                            return (file.name, new_title)
        
        # Strategy 3: Handle truncated filenames without brackets
        # Remove .json and try to find files with matching prefix
        if json_filename.endswith('.json'):
            base_without_json = json_filename[:-5]
            
            # The base might be truncated (e.g., "IMG_18~2.jp" instead of "IMG_18~2.jpg")
            # We need to find files that when you add an extension, match our truncated base
            try:
                files_in_dir = list(directory.iterdir())
            except OSError:
                return (None, new_title)
            
            for file in files_in_dir:
                if file.is_file() and file.name != json_filename:
                    # Check if removing the file's extension gives us something that starts with our base
                    file_stem = file.stem  # filename without extension
                    
                    # Check if the json base could be a truncated version of this file
                    # The json base should be a prefix of: filestem + possible partial extension
                    full_name_parts = file.name.rsplit('.', 1)
                    if len(full_name_parts) == 2:
                        stem, ext = full_name_parts
                        # Check all possible truncations
                        for i in range(len(ext) + 1):
                            potential_truncated = stem + ('.' + ext[:i] if i > 0 else '')
                            if potential_truncated == base_without_json:
                                return (file.name, new_title)
        
        # No matching file found
        return (None, new_title)
        
    except Exception:
        # Catch any unexpected errors
        return (None, None)