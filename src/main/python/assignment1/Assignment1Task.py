import os
import boto3
import logging
import pandas as pd
from dotenv import load_dotenv
from botocore.exceptions import ClientError
# Custom components
import Assignment1HtmlParser

# Logging setup
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='Assignment1Task.log', filemode='w')
# Config setup
load_dotenv()
ACCESS_KEY = os.environ.get('aws_access_key')
SECRET_KEY = os.environ.get('aws_secret_key')
BUCKET_NAME = os.environ.get('s3_bucket_name')
IN_FILE_PREFIX = "/raw"
OUT_FILE_PREFIX = "/processed"
OUT_FILE_FORMAT = 'csv'


class Assignment1Task(object):

    @staticmethod
    def connect_to_aws_s3(access_key, secret_key):
        aws_session = boto3.Session(aws_access_key_id=access_key,
                                    aws_secret_access_key=secret_key)
        s3_client = aws_session.client('s3')
        s3_resource = aws_session.resource('s3')
        return s3_client, s3_resource

    @staticmethod
    def read_from_s3(file_key, s3_client):
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        return response['Body']

    @staticmethod
    # Currently this supports 2 file formats csv and parquet
    def write_to_s3(data_df, path, file_format='csv'):
        if file_format == 'csv':
            data_df.to_csv(path, index=None)
        # elif file_format == 'parquet':
        #     data_df.to_parquet(path, index=None)
        else:
            logger.warning("***********Unsupported file format: %s", file_format)
            return False
        return True

    def run(self):
        logger.info("^^^^~~~~~~^^^^^ Here are the dragons ^^^^~~~~~~^^^^^")
        s3_client, s3_resource = self.connect_to_aws_s3(access_key=ACCESS_KEY,
                                                        secret_key=SECRET_KEY)
        s3_bucket = s3_resource.Bucket(BUCKET_NAME)

        object_summary_iterator = s3_bucket.objects.filter()
        logger.info("Found: " + str(object_summary_iterator))
        file_counter = 1
        for object_summary in object_summary_iterator: # s3_bucket.objects.filter(Prefix=IN_FILE_PREFIX):
            try:
                file_key = object_summary.key
                # Get html body from s3
                response_stream = self.read_from_s3(file_key, s3_client)
                html_body = response_stream.read()
                # Parse html to a list of type JobPost
                job_posts_lst = Assignment1HtmlParser.parse_html(stream=html_body)

                ###
                # Convert lst into a dataframe.
                # Used DF because it will take care of None which coping to CSV and compatible with other data formats.
                # If the size of records in input files grows this can be a bottleneck. But, for now it is managable.
                ###
                job_posts_df = pd.DataFrame(job_posts_lst)

                # Adding source file name for data lineage
                job_posts_df['source'] = file_key

                # Push to s3 bucket
                target_path = 's3://' + BUCKET_NAME + '/processed/' + str(file_counter) + '.' + OUT_FILE_FORMAT
                job_posts_df.to_csv(str(file_counter) + '.' + OUT_FILE_FORMAT, index=None)

                # Todo: Set up s3 bucket to store processed data
                # self.write_to_s3(job_posts_df, target_path, file_format=OUT_FILE_FORMAT)

            except ClientError as e:
                logger.warning("~~~~~~~~~~~~~~~~ Error for html query %s: %s", object_summary.key, e.response)
                continue

def main():
    print("In main")
    at1 = Assignment1Task()
    at1.run()

if __name__=="__main__":
    main()