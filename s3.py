import boto3

############find obj
def find_file(bucket_name, file_name):
    session = boto3.Session()
    client = session.client('s3')
    
    response = client.list_objects_v2(Bucket=bucket_name)

    find_objs = []
    
    for obj in response['Contents']:
        if file_name in obj['Key']:
            find_objs.append(obj['Key'])
    
    return find_objs

##
bucket_name = 'my-bucket'
file_name = 'zero'

find_files = find_file(bucket_name, file_name)
print(find_files)

############find obj - class
import boto3

class FindObjects:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = boto3.Session().client('s3')
    
    def find_objects_by_filename(self, filename):
        response = self.client.list_objects_v2(Bucket=self.bucket_name)
        matching_objects = []
        
        for obj in response.get('Contents', []):
            if filename in obj['Key']:
                matching_objects.append(obj['Key'])
        
        return matching_objects

##
bucket_name = 'sg-sm-test-998822'
filename = 'zero'
    
finder = FindObjects(bucket_name)
matching_files = finder.find_objects_by_filename(filename)
print(matching_files)



#####################list obj###############
def list_obj(bucket_name):
    session = boto3.Session()
    client = session.client('s3')
    
    response = client.list_objects(
        Bucket=bucket_name
    )
    
    for obj in response['Contents']:
        print('\n' 'object list : ' + obj['Key'])

bucket_name = 'my-bucket'  
res = list_obj(bucket_name)
print(res)


###########copy obj
def copy_obj(bucket_name, file_name, cp_bucket, cp_file):
    session = boto3.Session()
    client = session.client('s3')
    
    response = client.copy_object(
        Bucket=bucket_name,
        CopySource={'Bucket': cp_bucket , 'Key': cp_file},
        Key=file_name
    )

res = copy_obj(bucket_name, file_name, cp_bucket, cp_file)
print(str(file_name))

bucket_name = 'sg-sm-test-998822'
file_name = 'zero/cp_zero'
cp_bucket = 'sg-sm-test-998822'
cp_file = 'tt_tb01/zzeerroo.txt'


################delete obj
def delete_obj(bucket_name, file_name):
    session = boto3.Session()
    client = session.client('s3')

    response = client.delete_object(
        Bucket=bucket_name,
        Key=file_name,
    )

###############delete bucket
def delete_bucket(bucket_name):
    session = boto3.Session()
    client = session.client('s3')

    response = client.delete_bucket(
        Bucket=bucket_name
    )

############delete folder
import boto3

def delete_folder(bucket_name, folder_name):
    session = boto3.Session()
    client = session.client('s3')
    
    response = client.list_objects_v2(
        Bucket=bucket_name, 
        Prefix=folder_name
    )
    
    if 'Contents' in response:
        objects = [{'Key': obj['Key']} for obj in response['Contents']]
        response = client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})
        
        if 'Deleted' in response:
            deleted_objects = response['Deleted']
            print(f'Deleted objects in folder "{folder_name}":')
            for deleted_obj in deleted_objects:
                print(deleted_obj['Key'])
    else:
        print(f'Folder "{folder_name}" does not exist or is empty.')

# 버킷 이름과 삭제할 폴더 이름을 지정하여 폴더를 삭제합니다.
bucket_name = 'my-bucket'
folder_name = 'test'
delete_folder(bucket_name, folder_name)


############Create Bucket 
def create_bucket(bucket_name):
    session = boto3.Session()
    client = session.client('s3')
    response = client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': 'ap-northeast-2'
        },
    )