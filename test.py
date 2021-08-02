from reddit_etl.data_storage.block_level import upload_file_to_s3

if __name__ == '__main__':
    credential_file_path = 'c:\\Users\\dvschwab\\.aws\\credentials'
    profile = 'default'
    file = 'reddit-post-urls.txt'
    bucket = 'reddit-nlp-etl-spark-posts'

    upload_file_to_s3(credential_file_path, profile, file, bucket)