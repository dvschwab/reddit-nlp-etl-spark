import boto3

def load_aws_credentials_from_file(credential_location, profile):
    """
        Loads AWS credentials from the credential file, returning a two-element list containing the access key and the secret key
            args:
                credential_location: the location of the credentials file
                profile: the profile to verify
                (note that for security, neither of these are set to the boto3 defaults and must be passed even they are the default values)
        returns:
            credential_list: a two-element list containing (in order) the AWS access key and the AWS secret key
    """

    # Open the credentials file and retrieve the keys
    # Will skip newlines before the profile name,
    # but not between the name and each key
    # Will *not* skip whitespace before the profile name and key names,
    # but will trim key names if they are found
    try:
        with open(credential_location, "rt") as f:
            while line != '\n':
                line = f.read()
                if ('[' + line + ']') == profile:
                    aws_access_key_id = f.read()
                    aws_secret_access_key = f.read()
                    aws_access_key_id_stripped = aws_access_key_id.strip()   
                    aws_secret_access_key_stripped = aws_secret_access_key.strip()
                else:
                    aws_access_key_id = None
                    aws_secret_access_key = None
    except FileNotFoundError:
        print(f'The file {credential_location} could not be found. Check the file name to ensure it is correct')
    except IOError:
        print(f'The file {credential_location} could not be opened. Check that the program has permission to open this file and that the file is not corrupted')
    
    # Verify that the variables for both keys are not None.
    # This doesn't mean they contain the correct values, or even any values (including
    # the empty string). It just checks whether the profile was found, is formatted correctly,
    # (i.e. surrounded by brackets), and the two lines after the profile name contain *something*
    if (not aws_access_key_id or not aws_secret_access_key):
        print(f'The file {credential_location} does not contain either an aws access key id or an aws secret access key, or the profile {profile} is either not defined or defined incorrectly')
        raise RuntimeError
    else:
        credential_list = list(aws_access_key_id_stripped, aws_secret_access_key_stripped)
        return credential_list

def validate_aws_credentials(profile, credentials):
    """
        Checks that the AWS credentials are valid by calling the AWS IAM service
        args:
            profile: the profile name associated with the credentials. Note that for security there is no default profile (not even 'default')
            credentials: a list-like object containing the access key and secret key, in that order
        returns:
            True if the credentials are valid; otherwise, the response from the IAM service
    """
    
    