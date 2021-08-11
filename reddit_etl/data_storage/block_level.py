import boto3
from botocore.exceptions import ClientError
import reddit_etl.credentials.aws as aws

def upload_file_to_s3(credentials_file, profile, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket. Function core is defined at https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html as upload_file. Credential code and functions are mine.

    :param credentials_file: The location of the credentials file.
    :param profile: The profile associated with the credentials that will be used
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
    credentials = aws.load_aws_credentials_from_file(credentials_file, profile)
    # Upload the file
    s3_client = boto3.client('s3',
        aws_access_key_id = credentials[aws.AWS_ACCESS_KEY],
        aws_secret_access_key = credentials[aws.AWS_SECRET_KEY]
        )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        return False
    return True

def write_spark_dataframe_to_file(df, output_folder):
    """
        Writes a Spark DataFrame to a POSIX file system (e.g. Linux, etc.).
        Note that the arg *output_folder* is a folder, not the actual file name
        (which is determined by the Spark engine).

        :param df: Spark DataFrame to write
        :param output_folder: The name of the folder (NOT the file) to save the DataFrame to. The file itself is named by the Spark engine and will be written to the folder.
        :returns: True on success, False otherwise
    """

    df.write.csv(output_folder)

def load_file_to_hdfs():
    pass