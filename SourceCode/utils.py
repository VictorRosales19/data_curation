"""Utility functions for data acquisition, extraction, and directory management.
"""

import os

from zipfile import ZipFile

def unzip_files(zip_file_path: str, destination_path: str | None = None) -> None:
    """Unzip all files from a zip archive to a specified destination directory.

    Parameters
    ----------
    zip_file_path : str
        Path to the zip archive file.
    destination_path : str | None, optional
        Directory to extract files to. If None, extracts to the zip file's directory, by default None
    """
    destination_path = destination_path or os.path.dirname(zip_file_path)
    
    with ZipFile(zip_file_path, 'r') as zip_object:
        zip_object.extractall(path=destination_path)
        
    print(f"All files extracted from '{zip_file_path}' to '{destination_path}'.")


def ensure_dir(path: str) -> None:
    """Ensure that a directory exists; create it if it does not.
    Parameters
    ----------
    path : str
        Directory path to ensure existence of.
    """
    os.makedirs(path, exist_ok=True)