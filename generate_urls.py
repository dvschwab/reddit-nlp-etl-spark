# Generate the list of URLs that will be used to retrieve the Reddit posts
# Basically, just a loop that appends month/year strings to the stub
# Output is to *output_file* as straight text
# DVS 7/3/2021

url_stub = 'https://files.pushshift.io/reddit/comments/'
years = [2006, 2015]
output_file = 'reddit-urls-' + str(years[0]) + '-to-' + str(years[1]) + '.txt'

def generate_urls():

    file_urls = []

    # years[1] + 1 needed to get 2015
    for year in range(years[0], (years[1] + 1)):
        
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
                
            file_urls.append(url_stub + file)
    
    return file_urls

if __name__ == '__main__':

    reddit_urls = generate_urls()

    with open(output_file, 'wt') as f:
        for url in reddit_urls[0:-1]:
            f.write(url + '\n')
        f.write(reddit_urls[-1])