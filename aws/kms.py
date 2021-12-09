from .base_service import BaseService


class KMS(BaseService):
    """
    KMS Class, to encrypt/decrypt Lambda environment variables
    """
    def __init__(self, service_name='kms', app_config="app_config.ini"):
        """
        Initiate AWS service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        BaseService.__init__(self, service_name, app_config)

    def create_key(self, key_policy_document: str, key_tags: dict):
        """
        Create key in KMS
        :param key_policy_document: KMS key accessed policy document
        :param key_tags: tag
        :return: Response from create_key request
        """
        # Creating client key
        try:
            response = self.client.create_key(
                Description=self.config['description'],
                Policy=key_policy_document,
                KeyUsage='ENCRYPT_DECRYPT',
                CustomerMasterKeySpec='SYMMETRIC_DEFAULT',
                Origin='AWS_KMS',
                BypassPolicyLockoutSafetyCheck=False,
                Tags=key_tags
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f"Key is created")
            self.set_key_alias(response['KeyMetadata']['KeyId'])
            return response

    def set_key_alias(self, key_id: str):
        """
        Set Alias for CMK
        :param key_id: Key ID
        :return: Response from create_key request
        """
        try:
            response = self.client.create_alias(
                AliasName="alias/"+self.config['key_name'],
                TargetKeyId=key_id
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f"Alias {self.config['key_name']} is set for {key_id}")
            return response

    def delete_key(self, days_to_delete: int):
        """
        Set Alias for CMK
        :param days_to_delete: Key ID
        :return: Response from create_key request
        """
        key = self.get_key_by_alias(self.config['key_name'])

        if len(key) == 0:
            print(f"Key named {self.config['key_name']} doesn't exist")
        elif len(key) != 1:
            print(f"There are more than 1 key named {self.config['key_name']}, please manually remove")
        else:
            try:
                response = self.client.schedule_key_deletion(
                    KeyId=key[0]['TargetKeyId'],
                    PendingWindowInDays=days_to_delete
                )
            except Exception as e:
                self.logger.exception("")
                raise e
            else:
                print(f"Key is scheduled to delete in {days_to_delete} day(s)")
                return response

    def get_key_by_alias(self, alias_name: str) -> list:
        """
        Get key by alias
        :param alias_name: KMS key Alias name
        :return: A list of keys that match the alias
        """
        list_aliases = []

        try:
            response = self.client.list_aliases(
                Limit=1000
            )
            target_alias = f'alias/{alias_name}'

            while True:
                for alias in response['Aliases']:
                    if alias['AliasName'] == target_alias:

                        list_aliases.append(alias)

                if response['Truncated'] is False:
                    break

                marker = response['NextMarker']

                response = self.client.list_aliases(
                    Limit=1000,
                    Marker=marker
                )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f"Get {len(list_aliases)} alias(es)")
            return list_aliases
