from .base_service import BaseService


class STS(BaseService):
    """
    STS Class, including STS related operations
    """
    def __init__(self, service_name='sts', app_config="app_config.ini"):
        """
        Initiate AWS service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        BaseService.__init__(self, service_name, app_config)

    def get_account_id(self) -> str:
        """
        Get current account ID
        :return: account ID
        """
        try:
            response = self.client.get_caller_identity().get('Account')
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return response
