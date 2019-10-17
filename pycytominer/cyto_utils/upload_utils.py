"""
Uploading utility functions

Methods modified from official Figshare upload documentation:
https://docs.figshare.com/#upload_file
"""
import os
import json
import requests
import hashlib
from requests.exceptions import HTTPError


def raw_issue_request(token, method, url, data=None, binary=False):
    """
    Note: token must be generated on figshare settings
    https://nih.figshare.com/account/applications
    """
    headers = {"Authorization": "token {}".format(token)}
    if data is not None and not binary:
        data = json.dumps(data)
    response = requests.request(method, url, headers=headers, data=data)
    try:
        response.raise_for_status()
        try:
            data = json.loads(response.content)
        except ValueError:
            data = response.content
    except HTTPError as error:
        print("Caught an HTTPError: {}".format(error.message))
        print("Body:\n", response.content)
        raise
    return data


def issue_request(token, method, article_id, *args, **kwargs):
    base_url = "https://api.figshare.com/v2/{}".format(article_id)
    return raw_issue_request(token, method, base_url, *args, **kwargs)


def list_articles(token):
    result = issue_request(token, "GET", "account/articles")
    print("Listing current articles:")
    if result:
        for item in result:
            print(u"  {url} - {title}".format(**item))
    else:
        print("  No articles.")


def get_article_id(token, title):
    data = {"title": title}
    result = issue_request(token, "POST", "account/articles", data=data)
    print("Created article:", result["location"], "\n")

    result = raw_issue_request(token, "GET", result["location"])

    return result["id"]


def list_files_of_article(token, article_id):
    result = issue_request(token, "GET", "account/articles/{}/files".format(article_id))
    print("Listing files for article {}:".format(article_id))
    if result:
        for item in result:
            print("  {id} - {name}".format(**item))
    else:
        print("  No files.")


def get_file_check_data(file_name, chunk_size=1048576):
    with open(file_name, "rb") as fin:
        md5 = hashlib.md5()
        size = 0
        data = fin.read(chunk_size)
        while data:
            size += len(data)
            md5.update(data)
            data = fin.read(chunk_size)
        return md5.hexdigest(), size


def initiate_new_upload(token, article_id, file_name, chunk_size=1048576):
    endpoint = "account/articles/{}/files".format(article_id)

    md5, size = get_file_check_data(file_name, chunk_size)
    data = {"name": os.path.basename(file_name), "md5": md5, "size": size}

    result = issue_request(token, "POST", endpoint, data=data)
    print("Initiated file upload:", result["location"], "\n")

    result = raw_issue_request(token, "GET", result["location"])

    return result


def complete_upload(token, article_id, file_id):
    issue_request(
        token, "POST", "account/articles/{}/files/{}".format(article_id, file_id)
    )


def upload_parts(token, file_info):
    url = "{upload_url}".format(**file_info)
    result = raw_issue_request(token, "GET", url)

    print("Uploading parts:")
    with open(FILE_PATH, "rb") as fin:
        for part in result["parts"]:
            upload_part(token, file_info, fin, part)
    print


def upload_part(token, file_info, stream, part):
    udata = file_info.copy()
    udata.update(part)
    url = "{upload_url}/{partNo}".format(**udata)

    stream.seek(part["startOffset"])
    data = stream.read(part["endOffset"] - part["startOffset"] + 1)

    raw_issue_request(token, "PUT", url, data=data, binary=True)
    print("  Uploaded part {partNo} from {startOffset} to {endOffset}".format(**part))
