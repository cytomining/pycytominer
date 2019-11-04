"""
Upload files to online archives to assign DOI and version
"""

import os
import pandas as pd
from pycytominer.cyto_utils.upload_utils import (
    get_article_id,
    initiate_new_upload,
    upload_parts,
    complete_upload,
    list_files_of_article,
)


def figshare_upload(
    token, file_name, append=True, title="none", article_id="none", chunk_size=1048576
):
    """
    Create a new figshare article and upload to service

    Arguments:
    token - API token to authorize upload to your figshare account.
            Generate a token here: https://nih.figshare.com/account/applications
    file_name - location of the file to upload
    append - boolean if the file being uploaded should be appended to existing artile
    title - string indicating the title of the upload document
    article_id - string of existing figshare article accessible with API token
    chunk_size - how big each piece of the data to upload at one time
    """

    if append:
        assert article_id != "none", "article_id must be specified to append upload"
        try:
            list_files_of_article(token, article_id)
        except AttributeError:
            print("article_id: {} does not exist.".format(article_id))
    else:
        assert title != "none", "title must be specified for new uploads"
        # Generate an article identifier for the data location
        article_id = get_article_id(token, title)

    # Get md5sum and size of the article to upload
    file_info = initiate_new_upload(token, article_id, file_name, chunk_size)

    # Perform the upload in chunks
    upload_parts(token, file_info, file_name)

    # Finalize the upload
    complete_upload(token, article_id, file_info["id"])
