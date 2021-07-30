import requests
import validators
import bz2

def get_file(url, return_encoding='binary', verbose=True):
    """
    Retrieves a file from the supplied URL using the requests library.
    args:
      url: the URL specifying the resource to be retrieved
    returns:
      The content of the request response
    """

    # Validate return_encoding has legal value
    if return_encoding not in('text', 'binary'):
      print('return_encoding must be either "text" or "binary"')
      raise ValueError
    
    # Try to validate URL is well-formated
    # Raise warning if not, since this library is not always right
    try:
      validators.url(url)
    except:
      print(f'The URL {url} does not appear to be well-formed and should be validated. Execution will continue, but request may fail.')
      raise UserWarning

    # Request the resource from the URL
    # Handle common exceptions and (if verbose=True) print the response code and message
    try:
      response = requests.get(url)
    except ConnectionError:
      print(f'The connection to {url} cannot be established.')
    except TimeoutError:
      print(f'The connection timed out before the resource at {url} could be retrieved')
    else:
      if verbose:
        print(f'Response code {response.status_code}\nResponse {response.reason}')

    if return_encoding == 'text':
      return response.text
    elif return_encoding == 'binary':
      return response.content

def unzip_bz2_file_to_text(zipped_file):
    """
    Decompresses the supplied file, which must be in bzip2 format.
    Requires the bz2 library.
    args:
      zipped_file: the compressed file in bzip2 format
    returns:
      unzipped_file: a (potentially very long) string containing the
      uncompressed file contents.
    """

    try:
      unzipped_file = bz2.decompress(zipped_file).decode()
    except (UnicodeDecodeError, AttributeError):
      print(f'The file {zipped_file} is not encoded correctly or is not a bytes-like object')

    return unzipped_file