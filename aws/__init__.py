from .ec2 import EC2
from .events import Events
from .iam import IAM
from .kms import KMS
from .lambda_service import Lambda_service
from .rds import RDS
from .s3 import S3
from .storage_gateway import StorageGateway
from .securityhub import SecurityHub
from .ses import SES
from .organizations import Organizations
from .sts import STS
from .config_service import ConfigService
from .athena import Athena


__all__ = [
    'Athena',
    'EC2',
    'Events',
    'IAM',
    'KMS',
    'Lambda_service',
    'RDS',
    'S3',
    'StorageGateway',
    'SecurityHub',
    'SES',
    'Organizations',
    'STS',
    'ConfigService'
]
