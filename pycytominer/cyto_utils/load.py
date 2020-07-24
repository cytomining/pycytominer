import pandas as pd


def load_profiles(profiles):
    """
    Unless a dataframe is provided, load the given profile dataframe from path or string

    Arguments:
    profiles - location or actual pandas dataframe of profiles

    Return:
    pandas DataFrame of profiles
    """
    if not isinstance(profiles, pd.DataFrame):
        try:
            profiles = pd.read_csv(profiles)
        except FileNotFoundError:
            raise FileNotFoundError(f"{profiles} profile file not found")
    return profiles


def load_platemap(platemap, add_metadata_id=True):
    """
    Unless a dataframe is provided, load the given platemap dataframe from path or string

    Arguments:
    platemap - location or actual pandas dataframe of platemap file
    add_metadata_id - boolean if "Metadata_" should be appended to all platemap columns

    Return:
    pandas DataFrame of profiles
    """
    if not isinstance(platemap, pd.DataFrame):
        try:
            platemap = pd.read_csv(platemap, sep="\t")
        except FileNotFoundError:
            raise FileNotFoundError(f"{platemap} platemap file not found")

    if add_metadata_id:
        platemap.columns = [
            f"Metadata_{x}" if not x.startswith("Metadata_") else x
            for x in platemap.columns
        ]
    return platemap
