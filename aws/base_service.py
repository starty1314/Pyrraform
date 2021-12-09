import boto3
from aws.utils import logger
from aws.utils import section_loader, get_project_path


class BaseService(object):
    """
    aws Base Service class for service instance
    """
    def __init__(self, service_name: str, app_config: str = "app_config.ini"):
        """
        Initiate AWS Base service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        self.cwd = get_project_path()
        self.project_name = self.cwd.split('/')[-1]
        self.config_file = app_config
        self.logger = logger()

        self.service_name = service_name
        self.config = section_loader(self.config_file, service_name)
        self.session = self._get_aws_session()
        self.client = self.session.client(service_name)

        no_resource_service = [
            'athena',
            'lambda',
            'events',
            'storagegateway',
            'kms',
            'rds',
            'securityhub',
            'organizations',
            'sts',
            'ses',
            'config'
        ]
        if service_name in no_resource_service:
            self.resource = f"{service_name} doesn't support resource API."
        else:
            self.resource = self.session.resource(service_name)

    def _get_aws_session(self):
        """
        Get AWS session
        :return: AWS Session object
        """
        config = section_loader(self.config_file, "aws")
        if config:
            if "aws_session_token" in config:
                session = boto3.Session(
                    aws_access_key_id=config["aws_access_key_id"],
                    aws_secret_access_key=config["aws_secret_access_key"],
                    aws_session_token=config["aws_session_token"],
                    region_name=config["region"]
                )
            else:
                session = boto3.Session(
                    aws_access_key_id=config["aws_access_key_id"],
                    aws_secret_access_key=config["aws_secret_access_key"],
                    region_name=config["region"]
                )
        else:
            session = boto3.session.Session()

        return session

    def get_all_services(self):
        """
        Get all available AWS service names
        :return: A list of AWS service names
        """
        return self.session.get_available_services()
