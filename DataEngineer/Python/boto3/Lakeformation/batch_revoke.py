import boto3
import random
import string

############################################
############################################
# n개의 role, n개의 table 일괄삭제

def lakeformation_grant_table(role_arns, database_name, table_names):
    lakeformation = boto3.client('lakeformation')
    string_pool = string.ascii_letters + string.digits
    entries = []

    for table_name in table_names:
        for role_arn in role_arns:
            entries += [
                {
                    'Id': "".join([random.choice(string_pool) for i in range(24)]),
                    'Principal' : {
                        'DataLakePrincipalIdentifier' : role_arn
                    },
                    'Resource': {
                        'Table': {
                            'DatabaseName': database_name,
                            'Name': table_name
                        }
                    },
                    'Permissions': ['SELECT']
                }
            ]
    
    try:
        response = lakeformation.batch_grant_permission(
            Entries = entries
        )
        print(response)
    
    except Exception as e:
        print('@ ERROR: ' + str(e))
        raise e 