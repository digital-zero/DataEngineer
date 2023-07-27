import json 
import boto3 

from classes import (
    Role, 
    Policy,
    S3,
    Glue,
    Athena,
    Lakeformation,
    Sagemaker
)


def lambda_handler(event, context):
    print('###')
    print('###'+ str(event))
    print('###')

    try:
        project_id = event['project_id']
        users = event['userid_list']
    
    except Exception as e:
        print('@ ERROR :' + str(e))

        return {
            'status_code' : 202,
            'body' : {'Error' : 'Parameter is not available for creating project : ' + str(e)}
        }
    
    print('# IN (Create Project)')
    print('# IN (Project ID) :' + str(project_id))
    print('# IN (UserID List) :' + str(users))


    role = Role(project=project_id)
    policy = Policy(project=project_id)
    s3 = S3(project=project_id)
    glue = Glue(project=project_id)
    athena = Athena(project=project_id)
    lf = Lakeformation(project=project_id)
    sagemaker = Sagemaker(project=project_id)

    print(f'# Start:: Create User Role & Policy')
    user_roles = []
    try: 
        for user in users:
            role.user = user 
            policy.user = user 

            user_role_name = role.get_user_role()

            if user_role_name:
                print(f'## Project {project_id}, User Role (Exists): {user_role_name}')
                user_roles.append(user_role_name)
                continue

            user_role_name = role.ceate_user_role()
            print(f'## Project {project_id}, User Role : {user_role_name}')

            user_roles.append(user_role_name)
            
            user_policy_arn = policy.create_user_policy()
            print(f'## Project{project_id}, User Policy : {user_policy_arn}')

            policy.attach_policy_user_role(
                role_name = user_role_name,
                policy_arns = user_policy_arn
            )
    except Exception as e:
        return {
            'status_code' : 202,
            'body': {'Error': 'Error occured at creating user Role & Policy (Check User Role : ' + str(e)}
        }
    
    print(f'# End:: Create User Role & Policy')


    print(f'# Start:: Grnat Permission for Project')
    try:
        project_role_arn =  role.get_project_role(type='arn')
        lf.grant_lf_datafilter(role_arn=project_role_arn)
        lf.grant_lf_database(role_arn=project_role_arn)
    except Exception as e:
        return {
            'status_code' : 202,
            'body': {'Error': 'Error occured at Lakeformation Permission' + str(e)}
        }

    print(f'# End:: Grnat Permission for Project')


