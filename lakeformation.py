import json
import time
import boto3
import random, string

from classes.config import Configurator

class Lakeformation:
    def __init__(self, project):
        self.__project = project

        self.cfg = Configurator()
        self.lf = boto3.client('lakeformation')
    
    @property
    def project(self):
        return self.__project

    def grant_lf_datafilter(self, role_arn, filter_type = '01') -> None:
        if filter_type is None:
            filter_type = self.project[1:3]
        
        print(f'# (_grant_if_datafilter) filter type - {str(filter_type)}')

        try:
            response = self.lf.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': role_arn
                },
                Resource={
                    'DataCellsFilter': {
                        'DatabaseName': self.cfg.filter_master['database'],
                        'TableName': self.cfg.filter_master['table'],
                        'Name': ('wdd_bdp_dlf_df_' + filter_type)
                    }
                },
                Permissions = ['SELECT']
        )

        except Exception as e:
            print('@ ERROR : ' + str(e))
            raise e 
    

    def grant_lf_database(self, role_arn, database=None) -> None:
        if database is None:
            required_databases = [
                self.cfg.catalog_database_prefix + 'dlk_l1',
                self.cfg.catalog_database_prefix + 'dlk_l2_ds',
                self.cfg.catalog_database_prefix + 'dlk_l2_ml',
                self.cfg.catalog_database_prefix + 'dlk_vm',
                self.cfg.catalog_database_prefix + 'dlk_vnm',
                self.cfg.catalog_database_prefix + self.project
            ]
        else:
            required_databases = [database] if type(database) == 'str' else database

        print(f'# (grant_lf_database) Database List - {required_databases}')

        entries = []
        string_pool = string.ascii_letters + string.digits

        for database in required_databases:
            entries += [
                {
                    Principal={
                        'DataLakePrincipalIdentifier': role_arn
                    },
                    Resource={
                        'Table': {
                            'DatabaseName': database,
                            'TableWildcard': {}
                        }
                    },
                    'Permissions': ['SELECT']
                }
            ]

    print(f'# (grant_lf_database) Lakeformation Entries : {entries}')
    try: 
        response = self.lf.batch_grant_permissions(
            Entries=entries
        )
    except Exception as e:
        print('@ ERROR :' + str(e))
        raise e


    def grant_lf_table(self, role_arn, database=None, table=None) -> None:
        if database is None:
            required_databases = [
                self.cfg.catalog_database_prefix + 'dlk_l1',
                self.cfg.catalog_database_prefix + 'dlk_l2_ds',
                self.cfg.catalog_database_prefix + 'dlk_l2_ml',
                self.cfg.catalog_database_prefix + 'dlk_vm',
                self.cfg.catalog_database_prefix + 'dlk_vnm',
                self.cfg.catalog_database_prefix + self.project
            ]
        else:
            required_databases = [database] if isinstance(database, str) else database

        print(f'# (grant_lf_database) Database List - {required_databases}')

        entries = []

        for database in required_databases:
            if table is None:
                entry = {
                    'Principal': {
                        'DataLakePrincipalIdentifier': role_arn
                    },
                    'Resource': {
                        'Database': {
                            'Name': database
                        }
                    },
                    'Permissions': ['SELECT']
                }
            else:
                entry = {
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

            entries.append(entry)

        print(f'# (grant_lf_database) Lakeformation Entries : {entries}')
        try:
            response = self.lf.batch_grant_permissions(
                Entries=entries
            )
        except Exception as e:
            print('@ ERROR :' + str(e))
            raise e






























    def grnat_lf_table(self, role_arn, database, tables):
        entries = []

        if istable(tables, str):
            tables = [tables]
        
        for table in tables:
            entry = {
                Principal={
                    'DataLakePrincipalIdentifier': role_arn
                },
                Resource={
                    'Table': {
                        'DatabaseName': database,
                        'Name': tables
                    }
            },
                Permissions=['SELECT']
            }

            entries.append(entry)
        
        try:
            response = self.lf.batch_grnat_permissions(Entries=entries)
            print(f'Granted Lake Formation permissions for tables: {tables}')
        except Exception as e:
            print('@ ERROR :' + str(e))
            raise e
        

role_arn = 'arn'
database = ''
tables = ['','',''] 
grnat_lf_table(role_arn, database, tables)






















import json
import boto3

class LakeFormationPermission:
    def __init__(self, database_name, table_name, role_arn):
        self.database_name = database_name
        self.table_name = table_name
        self.role_arn = role_arn
        self.client = boto3.client('lakeformation')
    
    def grant_permissions(self):
        self.grant_lf_table(self.database_name)
        self.grant_lf_table(self.table_name)
        
    def grant_lf_table(self, table_name):
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
    database_name = 'test_db01'
    table_name = 'test_tb01'
    role_arn = 'arn:aws:iam::365187617475:role/LBD_TEST'
    
    grant = LakeFormationPermission(database_name, table_name, role_arn)
    grant.grant_permissions()
    
    return {
        'statusCode': 200,
        'body': 'Permissions granted successfully'
    }

import json
import boto3

class LakeFormationPermission:
    def __init__(self, database_name, table_name, role_arn):
        self.database_name = database_name
        self.table_name = table_name
        self.role_arn = role_arn
        self.client = boto3.client('lakeformation')
    
    def grant_permissions(self):
        self.grant_lf_table(self.database_name)
        self.grant_lf_table(self.table_name)
        
    def grant_lf_table(self, table_name):
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
    database_name = 'test_db01'
    table_name = 'test_tb01'
    role_arn = 'arn:aws:iam::365187617475:role/LBD_TEST'
    
    grant = LakeFormationPermission(database_name, table_name, role_arn)
    grant.grant_permissions()
    
    return {
        'statusCode': 200,
        'body': 'Permissions granted successfully'
    }