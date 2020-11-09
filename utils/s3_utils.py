from botocore.exceptions import NoCredentialsError
import boto3
from helpers import get_s3_keys as keys
import os
from botocore.exceptions import ClientError
import logging

'''
use like so:

from s3_utils import s3_utils
s3 = s3_utils()
s3.get_buckets()

'''

class s3_utils:
    def __init__(self):
        self.s3_client = boto3.client('s3',
             aws_access_key_id=keys()['ACCESS_KEY'],
             aws_secret_access_key=keys()['SECRET_KEY'])
        self.s3_resource = boto3.resource('s3',
                               aws_access_key_id=keys()['ACCESS_KEY'],
                            aws_secret_access_key=keys()['SECRET_KEY'])



    def get_buckets(self):

        list_of_buckets = []

        for bucket in self.s3_resource.buckets.all():
            list_of_buckets.append(bucket)
            print(bucket.name)

        return list_of_buckets


    def create_bucket(self, bucket_name):

        self.s3_resource.create_bucket(Bucket=bucket_name)

        print(bucket_name + ' created')


    def delete_object(self, bucket_name, object_name):
        self.s3_resource.Object(bucket_name, object_name).delete()

        print(object_name + ' deleted')


    def upload_object(self, file_name, bucket, object_name=None):

         # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name
        try:
            response = self.s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True


    def get_objects(self,bucket_name):

        my_bucket = self.s3_resource.Bucket(bucket_name)
        list_of_files = []
        for my_bucket_object in my_bucket.objects.all():
            list_of_files.append(my_bucket_object.key)

        return list_of_files


    def download_objects(self,bucket_name, filename):
        bucket = self.s3_resource.Bucket(bucket_name)
        obj = bucket.Object(filename)
        obj.download_file(filename)



    BUCKET = 'hd-fr-tweets'


    # # path = os.path.join('twitter','account_tweets')
    # path = '/Users/julian.dragoi/PycharmProjects/prototyping/twitter/account_tweets'
    # files = os.listdir(path)
    # print(files)
    # for i in files:
    #     print(i)
    #     upload_object(str(os.path.join(path,i)), BUCKET, object_name=i)
    #
    # for i in get_objects(BUCKET):
    #     download_objects(BUCKET, i, s3_resource)
