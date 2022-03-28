#utilities to interact with AWS S3

from cep_credentials import aws_test_access_id, aws_test_access_key, aws_test_bucket
import boto3


s3 = boto3.client('s3', aws_access_key_id=aws_test_access_id, aws_secret_access_key=aws_test_access_key)


def test_s3(bucket_name, key_id, key_secret):
    s3 = boto3.client('s3', aws_access_key_id=key_id, aws_secret_access_key=key_secret)
    try:
        buckets = s3.list_buckets()
        print(buckets)    
    except:
        return False
    
    # save file to S3 from string
    s3.put_object(Bucket=bucket_name, Key='test.txt', Body='Hello World!')
    # download created file from S3
    s3.download_file(bucket_name, Key='test.txt', Filename='test_downloaded.txt')

    # get file from S3 to string
    obj = s3.get_object(Bucket=bucket_name, Key='test.txt')
    print(obj['Body'].read().decode('utf-8'))

    # move test file from S3 to another path

    s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': 'test.txt'}, Bucket=bucket_name, Key='moved/test_{date_str}.txt')
    s3.delete_object(Bucket=bucket_name, Key='test.txt')



    s3.upload_file(
        Filename="lecturas_l.txt",
        Bucket=bucket_name,
        Key="lecturas_l.txt",
    )


    s3.download_file(
        Bucket=bucket_name,
        Key="lecturas_l.txt",
        Filename="lecturas_l_loop.txt",
    )

    # delete de last file from bucket
    s3.delete_object(
        Bucket=bucket_name,
        Key="lecturas_l.txt",
    )



if __name__ == "__main__":
    test_s3(aws_test_bucket, aws_test_access_id, aws_test_access_key)
