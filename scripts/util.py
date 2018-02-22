import os
import boto3
import pprint
import os.path
import shutil

# aws clients

def get_emr_client():

    return boto3.client('emr',
                        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
                        region_name=Config.REGION_NAME)


def get_s3_client():

    return boto3.client('s3',
                        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
                        region_name=Config.REGION_NAME)


# local commands

def delete_from_local(file_path):

    if os.path.exists(file_path):
        input_val = input(f'Delete local: {file_path}? (y/n) ')
        if input_val == 'y':
            if os.path.isfile(file_path):
                os.remove(file_path)
            else:
                shutil.rmtree(file_path)
        else:
            exit()


def local_shell(jar_file):

    cmd = f'''{Config.SPARK_DIR}/bin/spark-shell \
        --driver-memory 4096M \
        --executor-memory 4096M \
        --jars {jar_file}
    '''
    run_command(cmd, 'Failed local shell')


def local_submit(jar_file, class_name, args):

    cmd = f'''{Config.SPARK_DIR}/bin/spark-submit \
        --driver-memory 4096M \
        --executor-memory 4096M \
        --deploy-mode client \
        --class "{class_name}" \
        --master local[4] \
        {jar_file} {args}
    '''
    run_command(cmd, 'Failed local submit')


# run commands

def run_command(cmd, failure_msg):
    print(cmd)
    result = os.system(cmd)
    if result != 0:
        print(failure_msg)
        exit()


def build(proj_name):

    # creating jar files in lib: jar cvf data.jar data
    cmd = f'sbt -mem 4096 "project {proj_name}" clean assembly'
    run_command(cmd, 'Failed build')


def test(proj_name):

    cmd = f'sbt -mem 4096 "project {proj_name}" clean test'
    run_command(cmd, 'Failed test')


def pretty_print(input_dict):
    pprint.pprint(input_dict)


# s3 commands

def delete_from_s3(s3_bucket, s3_key, check=True):

    s3_client = get_s3_client()

    response = s3_client.list_objects(
        Bucket=s3_bucket,
        Prefix=s3_key)

    if 'Contents' in response:

        if check:
            input_val = input(f'Delete remote: s3://{s3_bucket}/{s3_key}? (y/n) ')

        if not check or input_val == 'y':

            while 'Contents' in response:
                for content in response['Contents']:
                    key = content['Key']
                    print(f'Delete: {key}')
                    response = s3_client.delete_object(
                        Bucket=s3_bucket,
                        Key=key)
                print(response)

                response = s3_client.list_objects(
                    Bucket=s3_bucket,
                    Prefix=s3_key)
        else:
            exit()


def upload_to_s3(file, s3_bucket, s3_key, check=True):

    delete_from_s3(s3_bucket, s3_key, check)

    s3_client = get_s3_client()

    data = open(file, 'rb')

    response = s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=data)

    pretty_print(response)
    print(f'Uploaded to s3://{s3_bucket}/{s3_key}')