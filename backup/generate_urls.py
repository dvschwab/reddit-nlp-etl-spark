# Generate the list of URLs that will be used to retrieve the Reddit posts
# Years default to 2006 - 2015, but this can be changed with args
# Output is to *output_file* as straight text with one URL per line

# This script could, in principle, be generalized to handle different URLs,
# but the reddit URLs are too specialized to do that now.

import argparse

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

if __name__ == '__main__':
    # Generates the URL list for the range of years provided by the user
    # and writes it to a file. If the file exists, it will be overwritten
    # with no warning.

    # parse args
    # get the output file name
    # the optional args may be used to alter the start and end year
    
    parser = argparse.ArgumentParser()
    parser.add_argument("output_file")
    parser.add_argument("--start_year", type = int, default = 2006)
    parser.add_argument("--end_year", type = int, default = 2015)
    args = parser.parse_args()

    reddit_urls = generate_urls(args.start_year, args.end_year)

    # Warn user if URL list is empty
    if len(reddit_urls) == 0:
        print('The list of URLs to write is empty')
        raise UserWarning

    try:
        with open(args.output_file, 'wt') as f:
            for url in reddit_urls[0:-1]:
                f.write(url + '\n')
            f.write(reddit_urls[-1])
    except IOError:
        print(f'There was an error writing to the file {args.output_file}. Verify the file name is legal and that the program has write permission for the destination')