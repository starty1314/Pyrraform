from .base_service import BaseService
from aws.utils import parameter_check


class Athena(BaseService):
    """
    Athena Class, including AWS Athena related operations
    """
    def __init__(self, service_name='athena', app_config="app_config.ini"):
        """
        Initiate AWS service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        BaseService.__init__(self, service_name, app_config)

    def list_databases(self):
        pass
