import time
import scripts.util as util

EMR_RELEASE = 'emr-5.11.0'

# creating the cluster

def cluster_start(steps=[], keep_alive=True, terminate_on_failure=False):

    emr_client = util.get_emr_client()

    response = emr_client.run_job_flow(
        Name='spark-cluster',
        LogUri=f's3://{Config.S3_BUCKET}/emr-log',
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
                    'Path': f's3://{Config.S3_BUCKET}/emr/emr_bootstrap_java_8.sh',
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
                            f's3://{s3_bucket}/{s3_key}'
                        ] + args.strip().split()
            }
        }
    ]


def steps_client(s3_bucket, s3_key, class_name, args, terminate_on_failure=False):

    jar_name = f'{str(int(round(time.time() * 1000)))}.jar'

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
                    f's3://{s3_bucket}/{s3_key}',
                    f'/mnt/{jar_name}'
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
                            f'/mnt/{jar_name}'
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


# cluster control

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
    print(f'Job ID: {job_id}')

    return job_id


def cluster_terminate():

    job_id = cluster_check_job()
    if job_id is None:
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
    print(f'ssh -i ./{Config.EC2_KEY_NAME}.pem -ND 8157 hadoop@{master_dns}')
    print(f'http://{master_dns}/ganglia/')
    print(f'http://{master_dns}:8088')
    for instance_dns in instances_dns:
        print(f'http://{instance_dns}:8042')
