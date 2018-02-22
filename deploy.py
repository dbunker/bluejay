import argparse
from config import Config
import scripts.cluster as cluster
import scripts.util as util

util.Config = Config
cluster.Config = Config

#################

# example pipelined commands to build, upload, and spin up EMR cluster with job,
# terminating upon error or completion, will exit if previous command fails:
# python deploy.py --project simple_count --build --job_upload --job_submit_cluster_full

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
    is_submit = in_arg('submit')

    if args.project == 'nlp':
        class_name = 'com.bluejay.nlp.NLPProcess'

        input_path = 'data/comments/RC_2015-05'
        output_path = 'data/nlp'

    elif args.project == 'simple_count':
        class_name = 'com.bluejay.simplecount.SimpleCountProcess'

        input_path = 'data/comments/RC_2015-05'
        output_path = 'data/simple_count'

    elif args.project == 'word_count':
        class_name = 'com.bluejay.wordcount.WordCountProcess'

        input_path = 'data/nlp'
        output_path = 'data/word_count'

    elif args.project == 'vec_example':
        class_name = 'com.bluejay.vecexample.VecExampleProcess'

        input_path = 'data/test_vec'
        output_path = 'data/test_vec_out'

    elif args.project == 'word_to_vec':
        class_name = 'com.bluejay.wordtovec.Word2VecProcess'

        input_path = 'data/word_count'
        output_path = 'data/word_to_vec'

    else:
        print(f'Invalid project: {args.project}')
        exit()

    proj_name = args.project
    jar_file = f'{proj_name}.jar'
    jar_path = f'{proj_name}/target/scala-2.11/{jar_file}'

    s3_bucket = Config.S3_BUCKET
    program_s3_key = f'emr/program/{jar_file}'

    if is_submit:

        prefix = f's3n://{s3_bucket}' if is_job else Config.LOCAL_DATA_DIR
        if args.path:
            prefix = f'{prefix}/{args.path}'

        if is_job:
            util.delete_from_s3(s3_bucket, f'{args.path}/{output_path}' if args.path else output_path)
        else:
            util.delete_from_local(f'{prefix}/{output_path}')

        input_key = f'{prefix}/{input_path}'
        output_key = f'{prefix}/{output_path}'
        number_partitions = '32' if is_job else '1'

        if args.partitions:
            number_partitions = args.partitions

        env = 'prod' if is_job else 'local'
        params = f'{input_key} {output_key} {number_partitions} {env}'

        print(f'Params: {params}')

    if args.build:
        util.build(proj_name)

    if args.test:
        util.test(proj_name)

    if args.cluster_start:
        cluster.cluster_start()

    if args.cluster_check:
        cluster.cluster_check_job()

    if args.cluster_terminate:
        cluster.cluster_terminate()

    if args.cluster_debug:
        cluster.cluster_debug()

    if args.job_upload:
        util.upload_to_s3(jar_path, s3_bucket, program_s3_key, False)

    if args.job_submit_cluster:
        cluster.job_submit_cluster(s3_bucket, program_s3_key, class_name, params)

    if args.job_submit_client:
        cluster.job_submit_client(s3_bucket, program_s3_key, class_name, params)

    if args.job_submit_cluster_full:
        cluster.job_submit_cluster_full(s3_bucket, program_s3_key, class_name, params)

    if args.job_submit_client_full:
        cluster.job_submit_client_full(s3_bucket, program_s3_key, class_name, params)

    if args.local_shell:
        util.local_shell(jar_path)

    if args.local_submit:
        util.local_submit(jar_path, class_name, params)


if __name__ == '__main__':
    main()
