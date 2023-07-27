import boto3
from datetime import datetime

logs_client = boto3.client( 'logs')
sagemaker_client = boto3.client (' sagemaker')

job_next_token = None
logGroupName = '/aws/sagemaker/ProcessingJobs'

while True:
    paging_list_job = sagemaker_client.list_processing_jobs(
        SortBy='CreationTime',
        SortOrder= 'Descending'
        NextToken=job_next_token
    ) if job_next_token else sagemaker_client.list_processing_jobs()
    jobs = paging_list_job['ProcessingJobSummaries']
    processing_job_names = [['ProcessingJobName'] for × in jobs]
    job_next_token = paging_list_job.get ('NextToken' )
    
    if not job_next_token:
        break
    for job_name in processing_job_names:
        response_describe_log_streams = logs_client.describe_log_streams(
            logGroupName=logGroupName, 
            logStreamNamePrefix=job_name
        )
    list_logStream = response_describe_log_streams ['logStreams']
    list_logStreamName - [['logStreamName'] for × in list_logStream]
    
    log_token = None

    while True:
        for logStreamName in list_logStreamName:
            batch_get_log = logs_client.get_log_events(
                logGroupName=logGroupName, logStreamName=logStreamName, 
                startFromHead=False, 
                nextToken=log_token
            ) if log_token else logs_client.get_log_events()

        log_token = batch_get_log('nextBackwardToken')
        if not log_token:
            break
        
        list_all_events_columns = []
        events_list = batch_get_log['events']
        print('# Job Name: ' + job_name)
        print('# log Stream Name:' + logStreamName)
        print('# Events Size :' + str(len(events_list)))
        
        if len(events_list) > 0:
            for events in events list:
                events_timestamp = events['timestamp']
                events_message = events['message']
            
                timestamp = datetime.fromtimestamp (events_timestamp/1000)
                events_date = timestamp.strftime("%Y-%m-%d%H:%M:%5")
                list_a_events_columns = [events_date, events_message]
                list_all_events_columns-append(list_a_events_columns)
                print(list_a_events_columns)


import boto3

# AWS Sagemaker 클라이언트 생성
sagemaker_client = boto3.client('sagemaker')

# AWS CloudWatch 클라이언트 생성
log_client = boto3.client('logs')

# 1. AWS Sagemaker의 processing job list 가져오기
def list_processing_jobs():
    jobs = []
    next_token = ''
    while True:
        response = sagemaker_client.list_processing_jobs(NextToken=next_token) if next_token else sagemaker_client.list_processing_jobs()
        jobs.extend(response['ProcessingJobSummaries'])
        next_token = response.get('NextToken')
        if not next_token:
            break
    return jobs

processing_jobs = list_processing_jobs()

# 2. processing job name을 활용하여 log stream name 얻기
def get_log_stream_name(job_name):
    response = log_client.describe_log_streams(
        logGroupName='/aws/sagemaker/ProcessingJobs',
        logStreamNamePrefix=job_name
    )
    if response['logStreams']:
        return response['logStreams'][0]['logStreamName']

# 3. cloudwatch에서 event log 가져오기
def get_all_log_events(log_stream_name):
    events = []
    next_token = ''
    while True:
        response = log_client.get_log_events(
            logGroupName='/aws/sagemaker/ProcessingJobs',
            logStreamName=log_stream_name,
            nextToken=next_token
        )
        events.extend(response['events'])
        next_token = response.get('nextToken')
        if not next_token:
            break
    return events

# 작업 목록의 모든 로그 스트림 및 이벤트 가져오기
for job in processing_jobs:
    job_name = job['ProcessingJobName']
    log_stream_name = get_log_stream_name(job_name)
    if log_stream_name:
        log_events = get_all_log_events(log_stream_name)
        print(f"Processing Job: {job_name}")
        print("Log Events:")
        for event in log_events:
            print(event['message'])
        print("\n")
