from .base_service import BaseService
from botocore.exceptions import ClientError, NoCredentialsError
import os


class S3(BaseService):
    """
    S3 class, to help with S3 related operations
    """
    def __init__(self, service_name='s3', app_config="app_config.ini"):
        """
        Initiate AWS service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        BaseService.__init__(self, service_name, app_config)

    def create_s3_bucket(self):
        """
        Create S3 object
        :return: Response from S3 bucket creation request
        """
        bucket = self.config['name']
        region = self.config['region']

        try:
            # Check if you have permissions to access the bucket
            self.resource.meta.client.head_bucket(Bucket=bucket)
            # Delete any existing objects in the bucket
            self.resource.Bucket(bucket).objects.delete()
        except NoCredentialsError as e:
            # print(f'Error: {e.response["Error"]["Code"]}, {e.response["Error"]["Message"]}')
            raise e
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            # print(error_code)
            if error_code == 404:
                # Bucket does not exist, so create it.
                # Do not specify a LocationConstraint if the region is us-east-1 -
                # S3 does not like this!!
                create_bucket_config = {}
                if region != "us-east-1":
                    create_bucket_config["LocationConstraint"] = region

                    try:
                        response = self.resource.create_bucket(Bucket=bucket,
                                                               CreateBucketConfiguration=create_bucket_config)
                    except Exception as e:
                        self.logger.exception("")
                        raise e
                    else:
                        print('Created bucket: ' + bucket)
                        return response
                else:
                    try:
                        response = self.resource.create_bucket(Bucket=bucket)
                    except Exception as e:
                        self.logger.exception("")
                        raise e
                    else:
                        print('Created bucket: ' + bucket)
                        return response

            else:
                print("Specify a unique bucket name. Bucket names can only contain lowercase letters, numbers, and hyphens.")
                print(f'It is possible that a bucket with the name {bucket} already exists, and you may not have permissions to access the bucket.')
                print(f'Error: {e.response["Error"]["Code"]}, {e.response["Error"]["Message"]}')

    def delete_s3_bucket(self):
        """
        Delete S3 bucket
        :return: Response from delete bucket request
        """
        try:
            response = self.client.delete_bucket(Bucket=self.config['name'])
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'Bucket {self.config["name"]} has been removed')
            return response

    def config_s3_bucket(self):
        """
        Configure S3 bucket
        :return: Response from put_public_access_block request
        """
        bucket_name = self.config['name']

        try:
            response = self.client.put_public_access_block(
                Bucket=bucket_name,
                # ContentMD5 = 'string',
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': True,
                    'IgnorePublicAcls': True,
                    'BlockPublicPolicy': True,
                    'RestrictPublicBuckets': True
                }
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'Public Access Block Configuration for S3 bucket - {bucket_name} has been created')
            return response

    def delete_s3_objects(self):
        """
        Delete S3 Objects
        :return: Response from delete bucket request
        """
        try:
            bucket = self.resource.Bucket(self.config['name'])
            response = bucket.objects.all().delete()
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'Objects in Bucket {self.config["name"]} have been removed')
            return response

    def create_bucket_lifecycle(self, lifecycleconfiguration: dict):
        """
        Create bucket lifecycle, delete old files
        :param lifecycleconfiguration: Life Cycle Configuration
        :return: Response from put_bucket_lifecycle_configuration request
        """

        try:
            response = self.client.put_bucket_lifecycle_configuration(
                Bucket=self.config['name'],
                LifecycleConfiguration=lifecycleconfiguration
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f"S3 lifecycle rule - {self.config['lifecycle_name']} is created")
            return response

    def upload_object(self, object_path: str):
        """
        Upload object to S3 bucket
        :param object_path: Object's path
        :return: Response from upload_fileobj request
        """
        key = object_path.split('\\')[-1]

        try:
            with open(os.path.join(self.cwd, object_path), 'rb') as f:
                response = self.client.upload_fileobj(f, self.config['name'], key)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f"Object {object_path} has been uploaded")
            return response
