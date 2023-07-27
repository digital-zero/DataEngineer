import boto3

def grant_lf_table(role_arn, database, table_names):
    client = boto3.client('lakeformation')
    
    for table_name in table_names:
        response = client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': role_arn
            },
            Resource={
                'Table': {
                    'DatabaseName': database,
                    'Name': table_name
                }
            },
            Permissions=['SELECT']
        )
        
        print(f"# (grant_lf_table) table List -  {table_name}")
    
    return

def lambda_handler(event, context):
    role_arn = 'arn:aws:iam::365187617475:role/lamda_lf_test'  
    database = 'db_1'
    table_name = ['tb_1', 'tb_2', 'tb_3']
    

    response = grant_lf_table(role_arn, database, table_name)
    print(response)