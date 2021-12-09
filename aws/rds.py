from .base_service import BaseService
import time
import datetime
from urllib.parse import quote_plus
import dateutil


class RDS(BaseService):
    """
    RDS class, to help with RDS related operations
    """
    def __init__(self, service_name='rds', app_config="app_config.ini"):
        """
        Initiate AWS service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        BaseService.__init__(self, service_name, app_config)

    def create_instance(self):
        """
        Create DB instance
        :return: Response from create_db_instance request
        """
        try:
            response = self.client.create_db_instance(
                DBInstanceIdentifier=self.config['db_identifier'],
                DBInstanceClass=self.config['db_class'],
                DBName=self.config['db_name'],
                Engine=self.config['engine_name'],
                AllocatedStorage=int(self.config['db_size']),
                EngineVersion=self.config['engine_version'],
                MasterUsername=self.config['db_user_name'],
                MasterUserPassword=self.config['db_user_password'],
                PreferredBackupWindow="10:12-10:42",
                BackupRetentionPeriod=7,
                DBParameterGroupName=self.config['db_parameter_group_name'],
                PreferredMaintenanceWindow="wed:03:01-wed:03:31",
                PubliclyAccessible=bool(self.config['publicly_accessible'])
            )

            while True:
                ctime = str(datetime.datetime.now(tz=(dateutil.tz.gettz('US/Eastern'))).strftime('%Y-%m-%d %H:%M:%S'))
                db_status = self.get_instance()['DBInstances'][0]['DBInstanceStatus']

                if db_status == 'available':
                    print(f"{ctime}: DB instance - {self.config['db_identifier']} is ready")
                    break
                else:
                    print(f"{ctime}: DB instance - {self.config['db_identifier']} status is {db_status}")
                    time.sleep(60)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return response

    def get_instance(self):
        """
        Get DB instance details
        :return: Response from describe_db_instances request
        """
        try:
            response = self.client.describe_db_instances(DBInstanceIdentifier=self.config['db_identifier'])
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return response

    def delete_instance(self):
        """
        Delete DB instance
        :return: Response from delete_db_instance request
        """
        try:
            response = self.client.delete_db_instance(DBInstanceIdentifier=self.config['db_identifier'], SkipFinalSnapshot=True)

            ctime = str(datetime.datetime.now(tz=(dateutil.tz.gettz('US/Eastern'))).strftime('%Y-%m-%d %H:%M:%S'))
            # check delete DB instance returned successfully
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"{ctime}: Successfully sent command to delete DB instance {self.config['db_identifier']}")
            else:
                print(f"{ctime}: Couldn't delete DB, Error code: {response['ResponseMetadata']['HTTPStatusCode']}")

            print(f"{ctime}: Waiting for restored db {self.config['db_identifier']} to be removed")

            retries = 1
            while True:
                try:
                    db_status = self.get_instance()['DBInstances'][0]['DBInstanceStatus']
                    print(f"Restored DB still deleting {self.config['db_identifier']} is initializing. Attempt {retries}")
                except self.client.exceptions.DBInstanceNotFoundFault:
                    print(f"DB instance - {self.config['db_identifier']} is deleted")
                    break
                else:
                    if retries == 20:
                        break

                    retries += 1
                    time.sleep(10)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return response

    def backup_data(self, bucket_name: str, iam_role_arn: str) -> bool:
        """
        Create DB snapshot
        :param bucket_name: Backup bucket name
        :param iam_role_arn: IAM Role ARN
        :return: True or False
        """
        try:
            response = self.client.create_db_snapshot(DBInstanceIdentifier=self.config['db_identifier'],
                                                      DBSnapshotIdentifier=self.config['db_identifier_backup'])

            # check Create DB instance returned successfully
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"Successfully created DB snapshot for {self.config['db_identifier']}")
            else:
                print("Couldn't create DB snapshot")

            print(f"waiting for db snapshot - {self.config['db_identifier']} to become ready")
            number_of_retries = 20
            snapshot_success = False
            for i in range(number_of_retries):
                time.sleep(30)
                snp_status = self.client.describe_db_snapshots(DBSnapshotIdentifier=self.config['db_identifier_backup'])['DBSnapshots'][0]['Status']
                if snp_status == 'available':
                    snapshot_success = True
                    print(f"DB snapshot - {self.config['db_identifier_backup']} is ready")
                    break
                else:
                    print(f"DB snapshot - {self.config['db_identifier_backup']} is initializing. Attempt {i}")

        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return snapshot_success

    def check_engine_version(self) -> bool:
        """
        Check engine version against requested version
        :return: True or False
        """
        try:
            response = self.client.describe_db_engine_versions(Engine=self.config['engine_name'])

            # check Describe Engine Versions returned successfully
            for item in response["DBEngineVersions"]:
                if item['EngineVersion'] == self.config['engine_version']:
                    print(f'Engine Name: {self.config["engine_name"]} and Engine Version: {self.config["engine_version"]} found')
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return True

    def restore_db(self, bucket_name: str, iam_role_arn: str) -> bool:
        """
        Restore DB snapshot
        :param bucket_name: S3 bucket name
        :param iam_role_arn: IAM Role ARN
        :return: True or False
        """
        try:
            response = self.client.restore_db_instance_from_s3(
                DBName=self.config['db_name'],
                DBInstanceIdentifier=self.config['db_identifier'],
                #AllocatedStorage=123,
                DBInstanceClass=self.config['db_class'],
                Engine=self.config['engine_name'],
                MasterUsername=self.config['db_user_name'],
                MasterUserPassword=self.config['db_user_password'],
                # DBSecurityGroups=[
                #     'string',
                # ],
                # VpcSecurityGroupIds=[
                #     'string',
                # ],
                # AvailabilityZone='string',
                # DBSubnetGroupName='string',
                PreferredMaintenanceWindow="wed:03:01-wed:03:31",
                DBParameterGroupName=self.config['db_parameter_group_name'],
                BackupRetentionPeriod=7,
                PreferredBackupWindow='10:12-10:42',
                # Port=123,
                # MultiAZ=True | False,
                EngineVersion=self.config['engine_version'],
                # AutoMinorVersionUpgrade=True | False,
                # LicenseModel='string',
                # Iops=123,
                # OptionGroupName='string',
                PubliclyAccessible=self.config['publicly_accessible'],
                Tags=[
                    {
                        'Key': 'Environment',
                        'Value': 'Dev'
                    },
                    {
                        'Key': 'Owner',
                        'Value': 'John Doe'
                    },
                    {
                        'Key': 'Team',
                        'Value': 'DevOps'
                    },
                    {
                        'Key': 'Application',
                        'Value': 'Infra related'
                    }
                ],
                # StorageType='string',
                # StorageEncrypted=True | False,
                # KmsKeyId='string',
                CopyTagsToSnapshot=True,
                # MonitoringInterval=123,
                # MonitoringRoleArn='string',
                # EnableIAMDatabaseAuthentication=True | False,
                SourceEngine=self.config['engine_name'],
                SourceEngineVersion=self.config['engine_version'],
                S3BucketName=bucket_name,
                S3Prefix=self.config['db_identifier'],
                S3IngestionRoleArn=iam_role_arn,
                # EnablePerformanceInsights=True | False,
                # PerformanceInsightsKMSKeyId='string',
                # PerformanceInsightsRetentionPeriod=123,
                # EnableCloudwatchLogsExports=[
                #     'string',
                # ],
                # ProcessorFeatures=[
                #     {
                #         'Name': 'string',
                #         'Value': 'string'
                #     },
                # ],
                UseDefaultProcessorFeatures=True,
                DeletionProtection=False
            )

            # check restore DB instance returned successfully
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"Successfully restored DB snapshot {self.config['']} to instance {self.config['']}")
            else:
                print("Couldn't restore DB snapshot")

            print(f"waiting for restored db {self.config['']} to become ready")
            number_of_retries = 20
            restore_success = False
            for i in range(number_of_retries):
                time.sleep(30)
                restored_status = self.client.describe_db_instances(DBInstanceIdentifier=self.config['db_identifier'])['DBInstances'][0]['DBInstanceStatus']
                if restored_status == 'available':
                    restore_success = True
                    print(f"Restored DB - {self.config['db_identifier']} is ready")
                    break
                else:
                    print(f"Restored DB - {self.config['db_identifier']} is initializing. Attempt {i}")
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return restore_success

    def save_snapshot_to_s3(self, snapshot_arn: str, bucket_name: str, iam_role_arn: str, kms_key_id: str):
        """
        Save DB snapshot to S3
        :param snapshot_arn: Snapshot ARN
        :param bucket_name: s3 Bucket Name
        :param iam_role_arn: IAM Role ARN
        :param kms_key_id: Kms_key_id key ID
        :return: Response from the start_export_task
        """
        ctime = str(datetime.datetime.now(tz=(dateutil.tz.gettz('US/Eastern'))).strftime('%Y%m%d%H%M%S'))
        try:
            response = self.client.start_export_task(
                ExportTaskIdentifier=f'db_snapshot_export_task_{self.config["db_identifier"]}_{ctime}',
                SourceArn=snapshot_arn,
                S3BucketName=bucket_name,
                IamRoleArn=iam_role_arn,
                KmsKeyId=kms_key_id,
                S3Prefix=self.config['db_identifier']
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f"DB Snapshot has been saved to S3 bucket - {bucket_name}")
            return response

    def get_temp_password(self, dbhostname: str, port: int, dbusername: str, url_encoded: bool = True) -> str:
        """
        Get temporary password
        :return: Temporary password
        """
        region = self.session.region_name

        try:
            password = self.client.generate_db_auth_token(Region=region,
                                                          DBHostname=dbhostname,
                                                          Port=port,
                                                          DBUsername=dbusername)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            if url_encoded:
                return quote_plus(password)
            else:
                return password
