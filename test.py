import boto3

def lakeformation_grant_table(database_name, table_names, role_arn):
    client = boto3.client('lakeformation')
    
    for table_name in table_names:
        response = client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': role_arn
            },
            Resource={
                'Table': {
                    'DatabaseName': database_name,
                    'Name': table_name
                }
            },
            Permissions=['SELECT']
        )
        
        print(f"Permission granted for table: {table_name}")
    
    return

def lambda_handler(event, context):
    database_name = 'db_1'
    table_name = 'tb_1', 'tb_2'
    role_arn = 'arn:aws:iam::365187617475:role/lamda_lf_test'  # 권한을 부여할 IAM 사용자의 ARN

    response = lakeformation_grant_table(database_name, table_name, role_arn)
    print(response)














def deep_search (needles, haystack):
    found = {}
    if type (needles) != type([]) :
        needles = [needles]

    if type(haystack) == type(dict ()) :
        for needle in needles:
            if needle in haystack.keys():
                found [needle] = haystack [needle]
            elif len(haystack.keys()) > 0:
                for key in haystack.keys():
                    result = deep_search(needle, haystack[key])
                    if result:
                        for k, v in result.items():
                        found[k] = v
    elif type(haystack) == type([]) :
        for node in haystack:
            result = deep_search(needles, node)
            if result:
                for k, v in result.items ():
                    found[k] = v
    return found