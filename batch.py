import boto3
import json
import random, string

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
        response_ser = json.dumps(list(response))
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
        response_ser = json.dumps(list(response))
    except Exception as e:
        print('@Error : ' + str(e))
        raise e