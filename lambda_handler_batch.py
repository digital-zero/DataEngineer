from lakeformation import *
from role import *

import json 
import boto3


def lambda_handler(event, context):

    # role_lists = event['project']
    # role_arns = []
        
    # for role_list in role_lists:
    #     project_id = role_list['project_id']
    #     auth_code = role_list['auth_code']
            
    #     role_arn = project_role(auth_code, project_id)
    #     role_arns.append(role_arn)
        
    # for role_arn in role_arns: 
    #     return role_arns
        
    
    table_names = ['tb_1','tb_2']
    database = 'test_2'
    role_arns = ['arn:aws:iam::365187617475:role/lamda_lf_test','arn:aws:iam::365187617475:role/service-role/lf_permission-role-7a8t0eg1','arn:aws:iam::365187617475:role/aaa_l2m']

    try:
        #response = batch_grant(role_arns, database, table_names)
        response = batch_revoke(role_arns, database, table_names)
        
        if response is None:
            response = []  
        
        response_ser = json.dumps(list(response))    
            
    except Exception as e:
        return {
            'status_code' : 500,
            'body' : 'failed'
        }
        
    return {
        'status_code' : 200,
        'body' : 'Success'
    }
    


def lambda_handler(event, context):
    
    table_names = ['tb1', 'tb2']
    database = 'test_2'
    role_arns = ['arn:aws:iam::123333:role/aaa_l2m', 'arn:aws:iam::12333:role/bbb_l2m']
   

    try:
        response = batch_grant(role_arns, database, table_names)
        #response = batch_revoke(role_arns, database, table_names)
        
        if response is None:
            response = []  
        
        response_ser = json.dumps(list(response))    
            
    except Exception as e:
        return {
            'status_code' : 500,
            'body' : 'failed'
        }
        
    return {
        'status_code' : 200,
        'body' : 'Success'
    }
