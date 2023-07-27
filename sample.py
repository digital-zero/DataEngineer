import boto3

class LakeFormationPermissionGranter:
    def __init__(self, database_name, table_name, role_arn):
        self.database_name = database_name
        self.table_name = table_name
        self.role_arn = role_arn
        self.client = boto3.client('lakeformation')
    
    def grant_permissions(self):
        database_resource = self._get_database_resource()
        table_resource = self._get_table_resource(database_resource)
        
        self._grant_permissions_to_principal(database_resource)
        self._grant_permissions_to_principal(table_resource)
        
    def _get_database_resource(self):
        response = self.client.get_resource(DatabaseName=self.database_name)
        return response['ResourceInfo']['ResourceArn']
    
    def _get_table_resource(self, database_resource):
        response = self.client.get_table(DatabaseName=database_resource, Name=self.table_name)
        return response['Table']['TableArn']
    
    def _grant_permissions_to_principal(self, resource_arn):
        response = self.client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': self.role_arn
            },
            Resource={
                'Table': {
                    'DatabaseName': self.database_name,
                    'Name': self.table_name
                }
            },
            Permissions=['SELECT']
        )
        return response

def lambda_handler(event, context):
    # 이벤트와 컨텍스트에 필요한 정보가 제공되어야 합니다.
    database_name = event['database_name']
    table_name = event['table_name']
    role_arn = event['role_arn']
    
    granter = LakeFormationPermissionGranter(database_name, table_name, role_arn)
    granter.grant_permissions()
    
    return {
        'statusCode': 200,
        'body': 'Permissions granted successfully'
    }



------------
import boto3

class LakeFormationPermissionGranter:
    def __init__(self, database_name, table_name, role_arn):
        self.database_name = database_name
        self.table_name = table_name
        self.role_arn = role_arn
        self.client = boto3.client('lakeformation')
    
    def grant_permissions(self):
        database_resource = self._get_database_resource()
        table_resource = self._get_table_resource(database_resource)
        
        self._grant_permissions_to_principal(database_resource)
        self._grant_permissions_to_principal(table_resource)
        
    def _get_database_resource(self):
        response = self.client.get_resource(DatabaseName=self.database_name)
        return response['ResourceInfo']['ResourceArn']
    
    def _get_table_resource(self, database_resource):
        response = self.client.get_table(DatabaseName=database_resource, Name=self.table_name)
        return response['Table']['TableArn']
    
    def _grant_permissions_to_principal(self, resource_arn):
        response = self.client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': self.role_arn
            },
            Resource={
                'Table': {
                    'DatabaseName': self.database_name,
                    'Name': self.table_name
                }
            },
            Permissions=['SELECT']
        )
        return response

def lambda_handler(event, context):
    # 이벤트와 컨텍스트에 필요한 정보가 제공되어야 합니다.
    database_name = event['test_db01']
    table_name = event['test_tb01']
    role_arn = event['arn:aws:iam::365187617475:role/LBD_TEST']
    
    granter = LakeFormationPermissionGranter(database_name, table_name, role_arn)
    granter.grant_permissions()
    
    return {
        'statusCode': 200,
        'body': 'Permissions granted successfully'
    }








import boto3

class LakeFormationPermissionGranter:
    def __init__(self, database_name, table_name, principal_arn):
        self.database_name = database_name
        self.table_name = table_name
        self.principal_arn = principal_arn
        self.client = boto3.client('lakeformation')
    
    def grant_permissions(self):
        self._grant_permissions_to_principal(self.database_name)
        self._grant_permissions_to_principal(self.table_name)
        
    def _grant_permissions_to_principal(self, resource_name):
        response = self.client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': self.principal_arn
            },
            Resource={
                'Table': {
                    'DatabaseName': self.database_name,
                    'Name': resource_name
                }
            },
            Permissions=['SELECT']
        )
        return response

def lambda_handler(event, context):
    # 이벤트와 컨텍스트에 필요한 정보가 제공되어야 합니다.
    database_name = event['database_name']
    table_name = event['table_name']
    principal_arn = event['principal_arn']
    
    granter = LakeFormationPermissionGranter(database_name, table_name, principal_arn)
    granter.grant_permissions()
    
    return {
        'statusCode': 200,
        'body': 'Permissions granted successfully'
    }

