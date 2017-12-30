import os
import argparse
import json
import boto3
import pprint
import time
from config import Config
import os.path
import shutil

EMR_RELEASE = 'emr-5.11.0'

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

#################

def cluster_start(steps=[], keep_alive=True, terminate_on_failure=False):

    emr_client = get_emr_client()

    response = emr_client.run_job_flow(
        Name='spark-cluster',
        LogUri='s3://' + Config.S3_BUCKET + '/emr-log',
        ReleaseLabel=EMR_RELEASE,
        Instances={
            'MasterInstanceType': 'm3.xlarge',
            'SlaveInstanceType': 'm3.xlarge',
            'InstanceCount': 3,
            'Ec2KeyName': Config.EC2_KEY_NAME,
            'KeepJobFlowAliveWhenNoSteps': keep_alive
        },
        Steps=([
            {
                'Name': 'SetupDebugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            }
        ] if terminate_on_failure else []) + steps,
        Applications=[
            {
                'Name': 'Spark'
            },
            {
                'Name': 'Ganglia'
            }
        ],
        BootstrapActions=[
            {
                'Name': 'Java8',
                'ScriptBootstrapAction': {
                    'Path': 's3://' + Config.S3_BUCKET + '/emr/emr_bootstrap_java_8.sh',
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='DataPipelineDefaultResourceRole',
        ServiceRole='DataPipelineDefaultRole'
    )

    print('cluster_start')
    pretty_print(response)

def steps_cluster(s3_bucket, s3_key, class_name, args, terminate_on_failure=False):
    return [
        {
            'Name': 'SparkProgramCluster',
            'ActionOnFailure': 'TERMINATE_CLUSTER' if terminate_on_failure else 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode',
                    'cluster',
                    '--executor-memory',
                    '8G',
                    '--executor-cores',
                    '4',
                    '--num-executors',
                    '3',
                    '--class',
                    class_name,
                    's3://' + s3_bucket + '/' + s3_key
                ] + args.strip().split()
            }
        }
    ]

def steps_client(s3_bucket, s3_key, class_name, args, terminate_on_failure=False):

    jar_name = str(int(round(time.time() * 1000))) + '.jar'

    return [
        {
            'Name': 'UploadProgram',
            'ActionOnFailure': 'TERMINATE_CLUSTER' if terminate_on_failure else 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'hdfs',
                    'dfs',
                    '-get',
                    's3://' + s3_bucket + '/' + s3_key,
                    '/mnt/' + jar_name
                ]
            }
        },
        {
            'Name': 'SparkProgramClient',
            'ActionOnFailure': 'TERMINATE_CLUSTER' if terminate_on_failure else 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode',
                    'client',
                    '--driver-memory',
                    '8G',
                    '--executor-memory',
                    '8G',
                    '--executor-cores',
                    '4',
                    '--num-executors',
                    '3',
                    '--class',
                    class_name,
                    '/mnt/' + jar_name
                ] + args.strip().split()
            }
        }
    ]

def job_submit_cluster_full(s3_bucket, s3_key, class_name, args):

    steps = steps_cluster(s3_bucket, s3_key, class_name, args)
    cluster_start(steps=steps, keep_alive=False)
    print('job_submit_cluster_full')

def job_submit_client_full(s3_bucket, s3_key, class_name, args):

    steps = steps_client(s3_bucket, s3_key, class_name, args)
    cluster_start(steps=steps, keep_alive=False)
    print('job_submit_client_full')

def job_submit(steps):

    job_id = cluster_check_job()

    emr_client = get_emr_client()
    response = emr_client.add_job_flow_steps(
        JobFlowId=job_id,
        Steps=steps
    )
    print('job_submit')
    pretty_print(response)

def job_submit_cluster(s3_bucket, s3_key, class_name, args):
    job_submit(steps_cluster(s3_bucket, s3_key, class_name, args))

def job_submit_client(s3_bucket, s3_key, class_name, args):
    job_submit(steps_client(s3_bucket, s3_key, class_name, args))

#################

def cluster_check_job():

    emr_client = get_emr_client()
    response = emr_client.list_clusters(ClusterStates=[
        'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'
    ])
    pretty_print(response)

    clusters = response['Clusters']
    if len(clusters) == 0:
        print('No clusters')
        return None

    if len(clusters) > 1:
        print('More than one cluster')
        return None

    job_id = clusters[0]['Id']
    print('Job ID: ' + job_id)

    return job_id

def cluster_terminate():

    job_id = cluster_check_job()
    if job_id == None:
        return

    emr_client = get_emr_client()
    response = emr_client.terminate_job_flows(
        JobFlowIds=[
            job_id
        ]
    )
    pretty_print(response)

def cluster_debug():

    job_id = cluster_check_job()

    emr_client = get_emr_client()
    response = emr_client.describe_cluster(
        ClusterId=job_id
    )
    pretty_print(response)

    if 'MasterPublicDnsName' not in response['Cluster']:
        print('Master does not exist')
        exit()

    master_dns = response['Cluster']['MasterPublicDnsName']

    response = emr_client.list_instances(
        ClusterId=job_id,
        InstanceGroupTypes=[
            'CORE',
        ]
    )
    pretty_print(response)

    instances_dns = [ instance['PublicDnsName'] for instance in response['Instances'] ]

    # use with AWS EMR foxyproxy rules
    print('')
    print('ssh -i ./' + Config.EC2_KEY_NAME + '.pem -ND 8157 hadoop@' + master_dns)
    print('http://' + master_dns + '/ganglia/')
    print('http://' + master_dns + ':8088')
    for instance_dns in instances_dns:
        print('http://' + instance_dns + ':8042')

def delete_from_local(file_path):

    if os.path.exists(file_path):
        input_val = input('Delete local: ' + file_path + '? (y/n) ')
        if input_val == 'y':
            if os.path.isfile(file_path):
                os.remove(file_path)
            else:
                shutil.rmtree(file_path)
        else:
            exit()

def delete_from_s3(s3_bucket, s3_key, check=True):

    s3_client = get_s3_client()

    response = s3_client.list_objects(
        Bucket=s3_bucket,
        Prefix=s3_key)

    if 'Contents' in response:

        if check:
            input_val = input('Delete remote: s3://' + s3_bucket + '/' + s3_key + '? (y/n) ')

        if not check or input_val == 'y':

            while 'Contents' in response:
                for content in response['Contents']:
                    key = content['Key']
                    print('Delete: ' + key)
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
    print('Uploaded to ' + 's3://' + s3_bucket + '/' + s3_key)

def local_shell(jar_file):

    cmd = Config.SPARK_DIR + '''/bin/spark-shell \
        --driver-memory 4096M \
        --executor-memory 4096M \
        --jars ''' + jar_file + '''
    '''
    run_command(cmd, 'Failed local shell')

def local_submit(jar_file, class_name, args):

    cmd = Config.SPARK_DIR + '''/bin/spark-submit \
        --driver-memory 4096M \
        --executor-memory 4096M \
        --deploy-mode client \
        --class "''' + class_name + '''" \
        --master local[4] \
        ''' + jar_file + ' ' + args + '''
    '''
    run_command(cmd, 'Failed local submit')

def build(local_dir):

    # creating jar files in lib: jar cvf data.jar data
    cmd = 'cd ' + local_dir + ' && sbt -mem 4096 clean assembly'
    run_command(cmd, 'Failed build')

def test(local_dir):

    cmd = 'cd ' + local_dir + ' && sbt -mem 4096 clean test'
    run_command(cmd, 'Failed test')

def run_command(cmd, failure_msg):
    print(cmd)
    result = os.system(cmd)
    if result != 0:
        print(failure_msg)
        exit()

def pretty_print(input_dict):
    pprint.pprint(input_dict)

#################

# example pipelined commands to build, upload, and spin up EMR cluster with job,
# terminating upon error or completion, will exit if previous command fails:
# python deploy.py --project word_count --build --job_upload --job_submit_cluster_full
def main():

    parser = argparse.ArgumentParser()

    parser.add_argument('--project', help='The project to operate on')
    parser.add_argument('--path', help='Set initial path for testing')
    parser.add_argument('--partitions', help='Set partitions')

    parser.add_argument('--build', action='store_true', help='Build scala project')
    parser.add_argument('--test', action='store_true', help='Test scala project')

    parser.add_argument('--local_submit', action='store_true', help='Submit job to localhost')
    parser.add_argument('--local_shell', action='store_true', help='Run Spark shell with project jar')

    parser.add_argument('--cluster_start', action='store_true', help='Start cluster')
    parser.add_argument('--cluster_check', action='store_true', help='Check cluster status')
    parser.add_argument('--cluster_terminate', action='store_true', help='Terminate cluster')
    parser.add_argument('--cluster_debug', action='store_true', help='Run cluster in debug mode')

    parser.add_argument('--job_upload', action='store_true', help='Upload job to running cluster')
    parser.add_argument('--job_submit_cluster', action='store_true', help='Upload cluster mode job')
    parser.add_argument('--job_submit_client', action='store_true', help='Upload client mode job')
    parser.add_argument('--job_submit_cluster_full', action='store_true', help='Start cluster and upload cluster mode job')
    parser.add_argument('--job_submit_client_full', action='store_true', help='Start cluster and upload client mode job')

    args = parser.parse_args()

    def in_arg(name):
        return any((value and name in key) for (key, value) in vars(args).items())

    is_job = in_arg('job')
    is_local = in_arg('local')
    is_submit = in_arg('submit')

    if args.project == 'nlp':
        class_name = 'NLPProcess'

        input_path = 'data/comments/RC_2015-05'
        output_path = 'data/nlp'

    elif args.project == 'simple_count':
        class_name = 'SimpleCountProcess'

        input_path = 'data/comments/RC_2015-05'
        output_path = 'data/simple_count'

    elif args.project == 'word_count':
        class_name = 'WordCountProcess'

        input_path = 'data/nlp'
        output_path = 'data/word_count'

    elif args.project == 'vec_example':
        class_name = 'VecExampleProcess'

        input_path = 'data/test_vec'
        output_path = 'data/test_vec_out'

    elif args.project == 'word_to_vec':
        class_name = 'Word2VecProcess'

        input_path = 'data/word_count'
        output_path = 'data/word_to_vec'

    else:
        print('Invalid project: ' + args.project)
        exit()

    proj_name = args.project
    local_dir = Config.LOCAL_DIR + '/' + proj_name
    jar_file = proj_name + '-assembly-1.1.jar'
    jar_path = local_dir + '/target/scala-2.11/' + jar_file

    s3_bucket = Config.S3_BUCKET
    program_s3_key = 'emr/program/' + jar_file

    if is_submit:
        if is_job:
            prefix = 's3n://' + s3_bucket
            if args.path:
                prefix = prefix + '/' + args.path
            delete_from_s3(s3_bucket, (args.path + '/' if args.path else '') + output_path)

            input_key = prefix + '/' + input_path
            output_key = prefix + '/' + output_path
            number_partitions = '32'
            if args.partitions:
                number_partitions = args.partitions
            params = input_key + ' ' + output_key + ' ' + number_partitions + ' prod'

        else:
            prefix = Config.LOCAL_DATA_DIR
            if args.path:
                prefix = prefix + '/' + args.path
            delete_from_local(prefix + '/' + output_path)

            input_key = prefix + '/' + input_path
            output_key = prefix + '/' + output_path
            number_partitions = '1'
            if args.partitions:
                number_partitions = args.partitions
            params = input_key + ' ' + output_key + ' ' + number_partitions + ' local'

        print('Params: ' + params)

    if args.build:
        build(local_dir)

    if args.test:
        test(local_dir)

    if args.cluster_start:
        cluster_start()

    if args.cluster_check:
        cluster_check_job()

    if args.cluster_terminate:
        cluster_terminate()

    if args.cluster_debug:
        cluster_debug()

    if args.job_upload:
        upload_to_s3(jar_path, s3_bucket, program_s3_key, False)

    if args.job_submit_cluster:
        job_submit_cluster(s3_bucket, program_s3_key, class_name, params)

    if args.job_submit_client:
        job_submit_client(s3_bucket, program_s3_key, class_name, params)

    if args.job_submit_cluster_full:
        job_submit_cluster_full(s3_bucket, program_s3_key, class_name, params)

    if args.job_submit_client_full:
        job_submit_client_full(s3_bucket, program_s3_key, class_name, params)

    if args.local_shell:
        local_shell(jar_path)

    if args.local_submit:
        local_submit(jar_path, class_name, params)

if __name__ == '__main__':
    main()
