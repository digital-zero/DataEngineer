import boto3

def grant_permissions(database, table_names, role_arns, permission):
    client = boto3.client('lakeformation')

    permissions = [
        {'Principal':{
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
        for table_name in table_names for role_arn in role_arns]

    response = client.batch_grant_permissions(Entries=permissions)

    return response

database = 'test_db'
table_names = ['tb1', 'tb2']
role_arns = [
    'arn:aws:iam::123456789::role/aaa/testlv1',
    'arn:aws:iam::123456789::role/bbb/testlv2'
]
permission = 'select'

response = grant_permissions(database, table_names, role_arns, permission)
print(response)



def batch_grant_lakeformation_permission(database, tables, role_arn):
    client = boto3.client('lakeformation')
    batch_permissions = []

    for table in tables:
        permission = {
            'Principal': {
                'DataLakePrincipalIdentifier': role_arn
            },
            'Resource': {
                'Table': {
                    'DatabaseName': database,
                    'Name': table
                }
            },
            'Permissions': ['SELECT']
        }
        batch_permissions.append(permission)
    
    response = client.batch_grant_permissions(
        Entries=batch_permissions
    )

    for result in response['Failures']:
        print(f"Failed to grant permission for table: {result['Resource']['Table']['Name']}")
    
    print("Batch grant permissions completed")
    
    return response


# def batch_grant_lakeformation_permission(database, tables, role_arn):
#     client = boto3.client('lakeformation')
#     batch_permissions = []

#     for table in tables:
#         permission = {
#             'Id': str(uuid.uuid4()),
#             'Principal': {
#                 'DataLakePrincipalIdentifier': role_arn
#             },
#             'Resource': {
#                 'Table': {
#                     'DatabaseName': database,
#                     'Name': table
#                 }
#             },
#             'Permissions': ['SELECT']
#         }
#         batch_permissions.append(permission)
    
#     response = client.batch_grant_permissions(
#         Entries=batch_permissions
#     )


