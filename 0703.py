from lakeformation import *
from role import *

import json 
import boto3
import traceback


def lambda_handler(event, context):
    table_names = ['tb_1', 'tb_2']
    database = event['database']
    role_lists = event['project']
    role_arns = []
        
    for role_list in role_lists:
        project_id = role_list['project_id']
        auth_code = role_list['auth_code']
            
        role_arn = project_role(auth_code, project_id)
        role_arns.append(role_arn)
        
    for role_arn in role_arns: 
        print(role_arns)
    
    
    #### GRANT #####
    try:
        response = batch_grant(role_arns, database, table_names)

    except Exception as e:
        return {
            'status_code' : 500,
            'body' : str(e)
        }
        
        
    ##### REVOKE #####
    try:
        response = batch_revoke(role_arns, database, table_names)
    
    except Exception as e:
        return {
            'status_code' : 500,
            'body' : str(e)
        }
        
    
    ##### RETURN #####    
    return {
        'status_code' : 200,
        'body' : 'Success' 
    }
    


import boto3
import random, string
import json

def batch_grant(role_arns, database, table_names):
    lakeformation = boto3.client('lakeformation')
    string_pool = string.ascii_letters + string.digits
    
    entries = []
    
    for table_name in table_names:
        for role_arn in role_arns:
            entries += [
                {
                    'Id': "".join([random.choice(string_pool) for i in range(24)]),
                    'Principal': {
                        'DataLakePrincipalIdentifier': role_arn
                    },
                    'Resource': {
                        'Table': {
                            'DatabaseName': database,
                            'Name': table_name
                        }
                    },
                    'Permissions': ['SELECT']
                }
            ]
    try:
        response = lakeformation.batch_grant_permissions(
            Entries = entries
        )
        if response is None:
            raise Exception('batch_grant_permissions failed')
        
        print(response)
        
    except Exception as e:
        print('@Error : ' + str(e))
        raise e


def batch_revoke(role_arns, database, table_names):
    lakeformation = boto3.client('lakeformation')
    string_pool = string.ascii_letters + string.digits
    entries = []
    
    for table_name in table_names:
        for role_arn in role_arns:
            entries += [
                {
                    'Id': "".join([random.choice(string_pool) for i in range(24)]),
                    'Principal': {
                        'DataLakePrincipalIdentifier': role_arn
                    },
                    'Resource': {
                        'Table': {
                            'DatabaseName': database,
                            'Name': table_name
                        }
                    },
                    'Permissions': ['SELECT']
                }
            ]
    try:
        response = lakeformation.batch_revoke_permissions(
            Entries = entries
        )
        
        if response is None:
            raise Exception('batch_revoke_permissions failed')
        
        print(response)
        
    except Exception as e:
        print('@Error : ' + str(e))
        raise e

