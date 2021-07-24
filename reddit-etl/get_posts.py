import requests
import bz2

def get_file(url):
    """
    Retrieves a file from the supplied URL using the requests library.
    Basically a wrapper for requests.get(), but abstracted as a function
    so type-checking and error-handling can be added if needed.

    Argument:
      url: the URL specifying the resource to be retrieved

    Returns:
      The content of the request response. In this invocation, this
      will be the compressed file of reddit posts at the supplied URL
    """
  
    response = requests.get(url)

    return response.content

def unzip_file(zipped_file):
    """
    Decompresses the supplied file, which must be in bzip2 format.
    Requires the bz2 library.

    Argument:
      The compressed file in bzip2 format

    Returns:
      A (potentially very long) string containing the uncompressed
      file contents.
    """

    unzipped_file = bz2.decompress(zipped_file).decode()

    return unzipped_file