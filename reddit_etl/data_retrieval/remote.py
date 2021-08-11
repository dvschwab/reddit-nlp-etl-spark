# Generate the list of URLs that will be used to retrieve the Reddit posts
# Years default to 2006 - 2015, but this can be changed with args
# Output is to *output_file* as straight text with one URL per line

# This script could, in principle, be generalized to handle different URLs,
# but the reddit URLs are too specialized to do that now.

import requests
import validators
import bz2

URL_STUB = 'https://files.pushshift.io/reddit/comments/'

def generate_urls(start_year, end_year):
    """
        generates URLs for each file hosted at the URL stub https://files.pushshift.io/reddit/comments/ and returns them as a list. Primary purpose is to handle slight differences
        in dates and file extensions.
        args: start_year and end_year, the range of years to include in the list (end_year inclusive). Set start_year = end_year if only one year is desired.
        returns: url_list, a list of generated URLs
    """

    # Validate both args are integers and the range of year
    # is between 2006 and 2020
    # Also that start_year <= end_year
    if not (isinstance(start_year, int) and isinstance(end_year, int)):
        print("Both start_year and end_year must be integers")
        raise TypeError
    if not (start_year >= 2006 and end_year <= 2020):
        print("Only the range of years from 2006 to 2020 is supported.")
        raise ValueError
    if start_year > end_year:
        print("The starting year cannot be greater than the ending year")
        raise ValueError
    

    file_urls = []

    # end_year + 1 needed to get 2015
    for year in range(start_year, end_year + 1):
        
        # Set extension by year for most files
        
        if year < 2018:
            extension = '.bz2'
        elif year < 2019:
            extension = '.xz'
        else:
            extension = '.zst'
        
        # Loop over each file and generate its URL
        for month in range(1,13):
            
            # Handle a few special cases    
            if (year == 2017 and month == 12):
                extension = '.xz'
            if (year == 2018 and month in [11,12]):
                extension = '.zst'

            # Create the file name, adding the leading zero
            # if the month is 1 - 9
            if month < 10:
                file = 'RC_' + str(year) + '-0' + str(month) + extension
            else:
                file = 'RC_' + str(year) + '-' + str(month) + extension

            file_urls.append(URL_STUB + file)
    
    return file_urls

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