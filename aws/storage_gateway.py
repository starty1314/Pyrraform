from .base_service import BaseService


class StorageGateway(BaseService):
    """
    Storage GateWay Class, to map S3 bucket to a CIFS share
    """
    def __init__(self, service_name='storagegateway', app_config="app_config.ini"):
        """
        Initiate AWS service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        BaseService.__init__(self, service_name, app_config)

    def create_nfs_share(self, kms_key_arn: str, iam_role_arn: str, bucket_name: str, tags: dict):
        """
        Create NFS share
        :param kms_key_arn: KMS key ARN
        :param iam_role_arn: IAM Role ARN
        :param bucket_name: S3 bucket name
        :param tags: tags
        :return: Response from create_nfs_file_share request
        """
        gateways = self.get_gateway_by_name()
        if len(gateways) == 0:
            print(f"There is no gateway matched the gateway name {self.config['name']}")
        elif len(gateways) > 1:
            print(f"There are {len(gateways)} gateways matched the gateway name {self.config['name']}")
        else:
            try:
                response = self.client.create_nfs_file_share(
                    ClientToken=self.config['client_id'],
                    NFSFileShareDefaults={
                        'FileMode': '0666',
                        'DirectoryMode': '0777',
                        'GroupId': 65534,
                        'OwnerId': 65534
                    },
                    GatewayARN=gateways[0]['GatewayARN'],
                    KMSEncrypted=True,
                    KMSKey=kms_key_arn,
                    Role=iam_role_arn,
                    LocationARN=f"arn:aws:s3:::{bucket_name}",
                    DefaultStorageClass='S3_STANDARD',
                    ObjectACL='bucket-owner-full-control',
                    ClientList=['0.0.0.0/0'],
                    Squash='RootSquash',
                    ReadOnly=False,
                    GuessMIMETypeEnabled=True,
                    RequesterPays=False,
                    Tags=tags
                )
            except Exception as e:
                self.logger.exception("")
                raise e
            else:
                print(f"NFS share has been created")
                return response

    def delete_share(self, bucket_name: str):
        """
        Delete file share
        :param bucket_name: S3 bucket name
        :return: Response from delete_file_share request
        """
        shares = self.get_share_by_gateway_and_bucket(bucket_name)
        if len(shares) == 0:
            print(f"There is no matched NFS share")
        elif len(shares) > 1:
            print(f"There are {len(shares)} NFS shares matched.")
        else:
            try:
                response = self.client.delete_file_share(FileShareARN=shares[0]['FileShareARN'],
                                                         ForceDelete=True)
            except Exception as e:
                self.logger.exception("")
                raise e
            else:
                print(f"NFS share has been deleted, S3 bucket name: {bucket_name}, Gateway: {self.config['name']}")
                return response

    def get_gateway_by_name(self) -> list:
        """
        Get gateway by gateway name
        :return: A list of gateways that match the gateway name
        """
        list_gateways = []
        try:
            paginator = self.client.get_paginator('list_gateways')

            response = paginator.paginate()

            for item in response:
                for gateway in item['Gateways']:
                    if gateway['GatewayName'] == self.config['name']:
                        list_gateways.append(gateway)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f"Get {len(list_gateways)} gateway(s)")
            return list_gateways

    def get_share_by_gateway_and_bucket(self, bucket_name: str) -> list:
        """
        Get file shares by gateway name and bucket name
        :param bucket_name: S3 bucket name
        :return: A list of file shares that match the gateway name and bucket name
        """
        gateways = self.get_gateway_by_name()

        if len(gateways) == 0:
            print(f"There is no gateway matched the gateway name {self.config['name']}")
        elif len(gateways) > 1:
            print(f"There are {len(gateways)} gateways matched the gateway name {self.config['name']}")
        else:
            list_shares = []
            try:
                paginator = self.client.get_paginator('list_file_shares')
                response = paginator.paginate(GatewayARN=gateways[0]['GatewayARN'])
                for item in response:
                    for share in item['FileShareInfoList']:
                        share_detail = self.client.describe_nfs_file_shares(FileShareARNList=[share['FileShareARN']])
                        if share_detail['NFSFileShareInfoList'][0]['LocationARN'] == f"arn:aws:s3:::{bucket_name}":
                            list_shares.append(share_detail['NFSFileShareInfoList'][0])
            except Exception as e:
                self.logger.exception("")
                raise e
            else:
                print(f"Get {len(list_shares)} share(s)")
                return list_shares
