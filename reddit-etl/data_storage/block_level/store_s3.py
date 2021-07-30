import boto3
from botocore.exceptions import ClientError
from credentials.aws import aws_constants, load_aws_credentials_from_file

def upload_file_to_s3(credentials_file, profile, file_name, bucket, object_name=None, ):
    """Upload a file to an S3 bucket. Function core is defined at https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html as upload_file. Credential code and functions are mine.

    :credentials_file: The location of the credentials file.
    :profile: The profile associated with the credentials that will be used
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, False otherwise

    (note that for security, neither the file nor the profile have defaults. Even if these arguments will correspond to the boto3 defaults, they must be passed explicitly)
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Get the credentials from the credentials file
    # Note that for security, neither of these default to the boto3 defaults
    # and must be explicitly passed even if that is the case
    credentials = load_aws_credentials_from_file(credentials_file, profile)
    # Upload the file
    s3_client = boto3.client('s3',
        credentials[aws_constants.AWS_ACCESS_KEY],
        credentials[aws_constants.AWS_SECRET_KEY]
        )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        return False
    return True