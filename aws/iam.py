from .base_service import BaseService
import json


class IAM(BaseService):
    """
    IAM class, to help with IAM related operations
    """
    def __init__(self, service_name='iam', app_config="app_config.ini"):
        """
        Initiate AWS service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        BaseService.__init__(self, service_name, app_config)

    def create_role(self, role_trust_policy: str, tags: dict):
        """
        Create required IAM roles
        :param role_trust_policy: Assume Role Policy document
        :param tags: tags
        :return: Response from IAM role creation request
        """
        path = self.config['role_path']
        role_name = self.config['role_name']
        description = self.config['role_description']

        try:
            response = self.client.create_role(
                Path=path,
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(role_trust_policy),
                Description=description,
                MaxSessionDuration=3600,
                Tags=tags
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'HTTPStatusCode is {response["ResponseMetadata"]["HTTPStatusCode"]} for creating role - {role_name}')
            return response

    def create_policy(self, policy_document: dict):
        """
        Create required IAM roles
        :param policy_document: Policy document
        :return: Response from IAM Policy creation request
        """
        policy_name = self.config['policy_name']
        path = self.config['policy_path']
        description = self.config['policy_description']

        try:
            response = self.client.create_policy(
                PolicyName=policy_name,
                Path=path,
                PolicyDocument=json.dumps(policy_document),
                Description=description
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'HTTPStatusCode is {response["ResponseMetadata"]["HTTPStatusCode"]} for creating policy - {policy_name}')
            return response

    def attach_policy_to_role(self):
        """
        Attach role to policy
        :return: Response from attach_role_policy request
        """
        iam_policy = self.get_policy()

        try:
            self.client.attach_role_policy(RoleName=self.config['role_name'],
                                           PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")

            response = self.client.attach_role_policy(RoleName=self.config['role_name'],
                                                      PolicyArn=iam_policy[0]['Arn'])
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'HTTPStatusCode is {response["ResponseMetadata"]["HTTPStatusCode"]} for attaching role {self.config["role_name"]} to policy {iam_policy[0]["PolicyName"]}')
            return response

    def delete_role(self):
        """
        Delete role
        :return: Response from delete_role request
        """
        try:
            response = self.client.delete_role(RoleName=self.config['role_name'])
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'Role {self.config["role_name"]} has been removed')
            return response

    def get_policy(self):
        """
        Get IAM Policy by policy name
        :return: Found Policy details
        """
        paginator = self.client.get_paginator('list_policies')
        all_policies = [policy for page in paginator.paginate() for policy in page['Policies']]
        policy = [p for p in all_policies if p['PolicyName'] == self.config['policy_name']]
        return policy

    def delete_policy(self):
        """
        Delete IAM Policy by policy name
        :return: Response from delete_policy request
        """
        policy = self.get_policy()
        if len(policy) == 0:
            print("Policy doesn't exist")
            return

        policy_arn = policy[0]['Arn']

        try:
            policy_versions = self.client.list_policy_versions(PolicyArn=policy_arn)
            for policy_version in policy_versions['Versions']:
                if policy_version['IsDefaultVersion']:
                    continue
                self.client.delete_policy_version(PolicyArn=policy_arn, VersionId=policy_version['VersionId'])

            response = self.client.delete_policy(PolicyArn=policy_arn)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'Policy {self.config["policy_name"]} has been removed')
            return response

    def detach_role_policy(self):
        """
        Detach role from policy
        :return: Response from detach_role_policy request
        """
        policy = self.get_policy()

        if len(policy) == 0:
            print("Policy doesn't exist")
            return

        policy_arn = policy[0]['Arn']
        try:
            self.client.detach_role_policy(RoleName=self.config['role_name'],
                                           PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
            response = self.client.detach_role_policy(RoleName=self.config['role_name'],PolicyArn=policy_arn)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'Role {self.config["role_name"]} has been detached from Policy {self.config["policy_name"]}')
            return response
