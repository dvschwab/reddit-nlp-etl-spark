# Generate the list of URLs that will be used to retrieve the Reddit posts
# Years default to 2006 - 2015, but this can be changed with args
# Output is to *output_file* as straight text with one URL per line

import argparse

URL_STUB = 'https://files.pushshift.io/reddit/comments/'

def generate_urls(start_year, end_year):

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
        
        # Loop over each file and print its name
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

    # get the output file name
    # the optional args may be used to alter the start and end year
    
    parser = argparse.ArgumentParser()
    parser.add_argument("output_file")
    parser.add_argument("--start_year", type = int, default = 2006)
    parser.add_argument("--end_year", type = int, default = 2015)
    args = parser.parse_args()

    reddit_urls = generate_urls(args.start_year, args.end_year)

    with open(args.output_file, 'wt') as f:
        for url in reddit_urls[0:-1]:
            f.write(url + '\n')
        f.write(reddit_urls[-1])