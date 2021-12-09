from .base_service import BaseService


class Organizations(BaseService):
    """
    Organizations Class, including Organizations related operations
    """
    def __init__(self, service_name='organizations', app_config="app_config.ini"):
        """
        Initiate AWS service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        BaseService.__init__(self, service_name, app_config)

    def get_account_info(self, account_id: str) -> dict:
        """
        Get account alias by account id
        :param account_id: aws account id
        :return: Account alias
        """
        try:
            response = self.client.describe_account(AccountId=account_id)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return response
