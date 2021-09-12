# defines constants used by the module

AWS_ACCESS_KEY = 0
AWS_SECRET_KEY = 1

def load_aws_credentials_from_file(credential_location, profile):
    """
        Loads AWS credentials from the credential file, returning a two-element list containing the access key and the secret key. Expects file to contain only the profile name, access key, and secret key (in that order) with no intervening newlines. Files with multiple profiles are not currently supported, but may be supported at a future date.
        
        :param credential_location: the location of the credentials file
        :param profile: the profile to verify
        :returns credential_list: a two-element list containing (in order) the AWS access key and the AWS secret key
    """

    # Open the credentials file and retrieve the keys
    # will *not* skip newlines
    # Will *not* skip whitespace before the profile name and key names,
    # but will trim key names if they are found
    try:
        with open(credential_location, "rt") as f:
            line = f.readline().rstrip('\n')
            if line == '[' + profile + ']':
                aws_access_key_id = f.readline().split('=')[1].strip()
                aws_secret_access_key = f.readline().split('=')[1].strip()
            else:
                aws_access_key_id = None
                aws_secret_access_key = None
    except FileNotFoundError:
        print(f'The file {credential_location} could not be found. Check the file name to ensure it is correct')
        return False
    except IOError:
        print(f'The file {credential_location} could not be opened. Check that the program has permission to open this file and that the file is not corrupted')
        return False
    
    # Verify that the variables for both keys are not None.
    # This doesn't mean they contain the correct values, or even any values (including
    # the empty string). It just checks whether the profile was found, is formatted correctly,
    # (i.e. surrounded by brackets), and the two lines after the profile name contain *something*
    if (not aws_access_key_id or not aws_secret_access_key):
        print(f'The file {credential_location} does not contain either an aws access key id or an aws secret access key, or the profile {profile} is either not defined or defined incorrectly')
        raise RuntimeError
    else:
        credential_list = [aws_access_key_id, aws_secret_access_key]
        return credential_list