import sys
import os
from typing import Union
from dotenv import load_dotenv


def get_project_path(name: Union[str, None] = None):
    """
    Gets the file path of a specific project by its name from the system's paths.

    This function iterates over all paths in the system's path variable and searches
    for the project directory name. If found, it returns the shortest matching path.
    If no match is found, it raises an error indicating the absence of the directory.

    Parameters:
        name: str
            The name of the project directory to locate.

    Returns:
        str
            The complete path of the project directory that matches the specified name.

    Raises:
        NotADirectoryError
            If no path containing the given project name is found.
    """
    list_paths = sys.path
    project_name = os.getenv("PROJECT_NAME", name)
    project_path = os.getenv("PROJECT_PATH", "")
    if project_path != "":
        return project_path
    path0 = ""
    for path in list_paths:
        if path.rfind(project_name) != -1:
            if len(path0) == 0 or (len(path0) > len(path)):
                path0 = path
            else:
                continue
        else:
            continue
    if len(path0) == 0:
        raise NotADirectoryError("Project directory not found")
    else:
        return path0


def list_files_extension(path: str, extension: str):
    return [
        f
        for f in os.listdir(path)
        if os.path.isfile(os.path.join(path, f)) and os.path.splitext(f)[1] == extension
    ]


def delete_files_extension(path: str, extension: str, keep_newest: bool = True):
    """
    Deletes files with a specific extension in a given folder.

    This function retrieves all files with a given extension in the specified
    folder. Optionally, it keeps the newest file if multiple files with the
    specified extension are found in the folder. All other matching files are
    deleted.

    Parameters:
        path: str
            The path to the folder where the files are to be searched and deleted.
        extension: str
            The file extension to filter files for deletion.
        keep_newest: bool (optional)
            Indicates whether the newest file should be preserved. Defaults to True.

    Raises:
        FileNotFoundError:
            Raised if the specified folder does not exist or is inaccessible.
        PermissionError:
            Raised if there is no permission to delete files in the specified folder.
    """
    # get only files with a certain extension in the given folder
    files = list_files_extension(path=path, extension=extension)
    # files with path
    files_path = [os.path.join(path, file) for file in files]
    if keep_newest and len(files) > 1:
        newest_file = max(files_path, key=os.path.getmtime)
        files_path.remove(newest_file)
    # delete files
    for file in files_path:
        os.remove(file)


def helper_save(save: Union[bool, str, None], path_true):
    """
    Helper function to determine and return the absolute normalised path based on the
    save flag or string path provided as input. This function ensures the path is
    properly formatted and absolute for further use.

    Parameters:
    save : bool | str | None
       Specifies whether to save, not save, or interpret a string as a file path.
       If None or boolean True, the path is resolved from 'path_true'.
       If it's a string, the provided string is treated and resolved as a file path.
    path_true : str
       The default path used when 'save' is None or True.

    Returns:
    str
       The absolute normalised path derived based on the value of 'save'.

    Raises:
    ValueError
       If the provided 'save' parameter is neither a boolean, a string, nor None.
    """
    if (isinstance(save, bool) and save) or save is None:
        save = os.path.abspath(os.path.normpath(path_true))
    elif isinstance(save, bool) and not save:
        pass
    elif isinstance(save, str):
        save = os.path.abspath(os.path.normpath(save))
    else:
        raise ValueError("Invalid input for save")
    return save


def helper_dir(path: str):
    """
    Helper function to create a directory if it does not already exist.

    Parameters
    ----------
    path (str): The path to the directory to validate or create.
    Must be a string.

    Raises
    ------
    AssertionError: If `path` is not a string.
    """
    assert isinstance(path, str), "path must be a string"
    path = os.path.abspath(os.path.normpath(path))
    if os.path.exists(path):
        pass
    else:
        os.mkdir(path)


def load_env_variables(
    path_dotenv_file: Union[str, None] = None, project_name: str = "CeleryplusPython"
):
    """
    Load environment variables from a .env file.

    This function handles loading environment variables defined in a specified
    `.env` file or a default project directory location. If the `path_dotenv_file`
    is not provided, the function does not perform any operation. If the file
    does not exist at the specified path, it raises a FileNotFoundError.

    Arguments:
        path_dotenv_file: Union[str, None]
            The path to the `.env` file. If not provided, no environment variables
            are loaded. It should be a string representing the path to the file or
            None.
        project_name: str
            The name of the project. Used to generate the default project directory
            when locating the `.env` file. Defaults to "CeleryplusPython".

    Raises:
        FileNotFoundError
            If the `.env` file is not found at the specified path.
        ValueError
            If the `path_dotenv_file` argument is neither a string nor None.
    """
    # 0: Load environment variables
    if path_dotenv_file is None:
        pass
    elif isinstance(path_dotenv_file, str):
        project_path = get_project_path(name=project_name)
        env_path = os.path.join(project_path, path_dotenv_file)
        out = load_dotenv(env_path)
        if out:
            pass
        else:
            raise FileNotFoundError(f"{env_path}: .env file not found")
    else:
        raise ValueError("Invalid input for path_dotenv_file")


if __name__ == "__main__":
    get_project_path("CeleryplusPython")
