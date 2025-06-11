__all__ = ["get_utils_path", "get_src_path", "get_outside_path", "get_download_path", "get_download_sgf_path","get_db_path", "get_r_icaro_path", "get_slave_path"]

import os
import inspect


# --------------------------------------------------
def get_utils_path():
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    return dir_path

# --------------------------------------------------
def get_src_path():
    dir_path = get_utils_path()
    dir_path = os.path.dirname(dir_path)
    return dir_path

# --------------------------------------------------
def get_outside_path():
    dir_path = os.path.dirname(os.path.dirname(get_src_path()))
    return dir_path

# --------------------------------------------------
def get_download_path():
    dir_path = os.path.join(
        get_outside_path(),'Reportes Descargados'
    )
    return dir_path


# --------------------------------------------------
def get_download_sgf_path():
    dir_path = os.path.join(
        get_download_path(),'Sistema Gestion Financiera'
    )
    return dir_path

# --------------------------------------------------
def get_db_path():
    db_path = os.path.join(get_outside_path() ,'Python Output')
    db_path = os.path.join(db_path, 'SQLite Files')
    return db_path

# --------------------------------------------------
def get_r_icaro_path():
    dir_path = r'\\192.168.0.149\Compartida CONTABLE\R Apps (Compartida)\R Output\SQLite Files'
    return dir_path

# --------------------------------------------------
def get_slave_path():
    dir_path = r'\\192.168.0.149\Compartida CONTABLE\Slave'
    return dir_path

# --------------------------------------------------
def main():
    """Make a jazz noise here"""

    print(f"Utils Path: {get_utils_path()}")
    print(f"Src Path: {get_src_path()}")
    print(f"Outside Path: {get_outside_path()}")
    print(f"Download Path: {get_download_path()}")
    print(f"Download SGF Path: {get_download_sgf_path()}")
    print(f"DB Path: {get_db_path()}")
    print(f"R Icaro Path: {get_r_icaro_path()}")
    print(f"Slave Path: {get_slave_path()}")

# --------------------------------------------------
if __name__ == "__main__":
    main()

    # From /invicofapy

    # poetry run python -m src.utils.hangling_path