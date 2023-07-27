import boto3
from datetime import date, timedelta, datetime

############################################
############################################
# 특정 naming의 DB가 가진 table을 일별 삭제 

def table_delete ():
    glue_client = boto3.client( 'glue')

    # yesterday 
    yesterday = date.today() - timedelta(days=1)
    yesterday_date = yesterday.strftime("%Y-%m-%d")

    next_token = None

    # 데이터베이스 List - paging 처리
    while True:
        if next_token:
            database_response = glue_client.get_databases(Next_Token = database_response['NextToken'] )

        else:
            database_response = glue_client.get_databases()
    
        database_list.extend(database_response[ 'DatabaseList '])

        if 'NextToken' in database_response:
            next_token = database_response['NextToken']
        else:
            break

    if database_list:
        tables_deleted = False 
        for database in database_list:
            database_name = database ['Name']

            # 특정 DB 만 삭제할 것 
            if 'DBNAME' in database_name:
                tables_response = glue_client.get_tables(
                    DatabaseName = database_name
                )

                print(f'DB Name : {database _name}")

                # 프로젝트 DB의 테이를 리스트 : Name, Create date
                table_list = tables_response['TableList']
                for table in table list:
                    table_name = table['Name']
                    create_time = table['CreateTime']
                    Create_date = datetime.strftime(create_time, "%Y-%m-%d")

                if create_date = yesterday_date:
                    glue_client.delete_table(
                        DatabaseName-database_name, 
                        Name=table_name
                    )

                    print(f'Table Name: {table_name}, Create date: {create date}') 
                    print(f'-> {table name} of {database _name} Deleted')
                          
                tables_deleted = True

            if not tables_deleted:
                print(f'No tables were created yesterday')

    else:
        print (f 'No Database')


################################################################
import boto3
from table_delete import *


def lambda_handler(event, context):
    try:
        table_delete()
    except:
        return{
            'status code': 500,
            'body': 'Error: Error occured at table delete'
        }
            'status code': 500,
    
    return {
        'status code': 200, 
        'body' : 'Table delete Completed'
    }
