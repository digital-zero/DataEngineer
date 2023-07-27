import boto3
from datetime import datetime


############################################
############################################
# SageMaker job별로 event log 확인
# Processing, traning, transform .....

logs_client = boto3.client( 'logs')
sagemaker_client = boto3.client (' sagemaker')

Logs_client - boto3.client ('Logs")
sagemaker_client - boto3.client('sagemaker")

### job 1ist 받아오기
# 1 ) processing job list 가져오기
# next_token을 이용해서 all processing j0b을 가져옴

def list_processing_jobs():
    jobs = []
    next_token = None
    
    while True:
        if next_token:
        job_response = sagemaker_client.list_processing jobs(Next_Token=next_token)
        else:
            job_response = sagemaker_client.list_processing jobs()

        jobs.extend(job_response['ProcessingJobSummaries'])
        next_token = job_response.get('NextToken')
    
        if not next_token:
            break
            
    return jobs

processing_jobs = list_processing_jobs()

##############################################
# 2 ) processing job의 stream name 가져오기
# processing job name을 이용하여 log stream name 을 가져음

def get_log(job_name):
    stream_response = logs_client.describe_log_streams( 
        logGroupName = '/aws/sagemaker/ProcessingJobs',
        logStreamNamePrefix = job_name
    )

    if stream_response['logStreams']:
        return stream_response['logStreams'][0]['logStreamName']


# 3 ) processang Job의 stream name으로 event log 가져보기
# next_token을 이용해서 all e를 가져vent log 가져옴
# startFromHead=False을 통해서 가장 최근 log부터 가져옴

def get_all_log_event(log_stream_name):
    events = []
    next_token = None
    while True:
        if next_token:
            log_response = logs_client.get_log_events(
                logGroupName = '/aws/sagemaker/ProcessingJobs',
                logStreamName = log_stream_name,
                startFromHead = False, 
                nextToken = next_token
            )
        else:
            log_response = logs_client.get_log_events(
                logGroupName = '/aws/sagemaker/ProcessingJobs',
                logStreamName = log_stream_name
            )
         
        events.extend(log_response['events'])
        next_token = log_response.get('nextToken')

        if not next_token:
            break
    
    return events
#######################################################
# 4 ) input
# Processine Job name, events_date, events_message 순서로 값을 가져음 
# timestamp 값은 date로 변환 ex) 1689732163506 2023-07-19 02:02:43

for job in processing_jobs:
    job_name = job['ProcessingJobName']
    log_stream_name = get_log(job_name)
    if log_stream_name:
        log_events = get_all_lo_event(log_stream_name)
        print(f'Processing Job: {job_name}')
        print("Log Events : ")
        
        list_all_events_columns = []

        for event in lo_events:
        events_timestamp = event['timestamp']
        events_message = event['message']
        
        timestamp = datetime.fromtimestamp(events_timestamp/1000)
        events_date = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        list_a_events_columns = [events _date, events_message] 
        list_all_events_columns.append(list_a_events_columns)

        print(list_a_events_columns)