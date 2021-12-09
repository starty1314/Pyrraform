from .base_service import BaseService


class EC2(BaseService):
    """
    EC2 class, to help with EC2 related operations
    """
    def __init__(self, service_name='ec2', app_config="app_config.ini"):
        """
        Initiate AWS service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        BaseService.__init__(self, service_name, app_config)

    def create_instance(self):
        """
        Create EC2 instance
        :return: Response from create_instance request
        """
        image = self.get_latest_ami()

        key_pair = self.create_ec2_keypair()

        try:
            response = self.resource.create_instances(
                BlockDeviceMappings=[
                    {
                        'DeviceName': '/dev/xvda',
                        # 'VirtualName': 'string',
                        'Ebs': {
                            'DeleteOnTermination': True,
                            # 'Iops': 123,
                            # 'SnapshotId': 'string',
                            'VolumeSize': 30,
                            # 'VolumeType': 'standard' | 'io1' | 'gp2' | 'sc1' | 'st1',
                            # 'KmsKeyId': 'string',
                            # 'Encrypted': True | False
                        },
                        # 'NoDevice': 'string'
                    },
                ],
                ImageId=image['ImageId'],
                MinCount=1,
                MaxCount=1,
                InstanceType=self.config['instance_type'],
                KeyName=key_pair.key_name,
                TagSpecifications=[
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            {
                                'Key': 'ServerName',
                                'Value': self.config['name']
                            },
                        ]
                    },
                ],
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'EC2 instance - {self.config["name"]} is under construction')
            return response

    def delete_instance(self, server_name: str = None):
        """
        Delete EC2 instance by server name
        :param server_name: Optional: Server name
        :return: Response from terminate_instances request
        """
        if server_name is None:
            instances = self.get_instance_detail(
                self.config['name'])["Reservations"][0]["Instances"]
        else:
            instances = self.get_instance_detail(
                server_name)["Reservations"][0]["Instances"]

        if len(instances) == 0:
            print(f'There is no instance to delete')
        elif len(instances) > 1:
            print(f'{len(instances)} EC2 instances are found, please delete manually')
        else:
            try:
                response = self.client.terminate_instances(
                    InstanceIds=[instances[0]['InstanceId']],
                    DryRun=False
                )
            except Exception as e:
                self.logger.exception("")
                raise e
            else:
                print(f'EC2 instance - {self.config["name"]} is deleting')
                return response

    def create_ec2_keypair(self, key_pair_name: str = None):
        """
        Create EC2 key pair
        :param key_pair_name: Optional key_pair_name
        :return: Key pair object
        """
        if key_pair_name is None:
            key_pair_name = f'{self.config["key_pair_prefix"]}{self.config["name"]}'

        # create a file to store the key locally
        outfile = open(f'{key_pair_name}.pem', 'w')

        try:
            # call the BOTO3 ec2 function to create a key pair
            key_pair = self.resource.create_key_pair(KeyName=key_pair_name)

            # capture the key and store it in a file
            key_pair_out = str(key_pair.key_material)
            outfile.write(key_pair_out)
        except Exception as e:
            self.logger.exception("create_ec2_keypair")
            raise e
        else:
            print(f'Key pair - {key_pair_name} has been created')
            return key_pair

    def delete_ec2_keypair(self, key_pair_name: str = None):
        """
        Delete EC2 key pair
        :param key_pair_name: Key pair name
        :return: Response from delete request
        """
        if key_pair_name is None:
            key_pair_name = self.config['key_pair_prefix'] + \
                self.config['name']

        try:
            key_pair = self.resource.KeyPair(key_pair_name)

            response = key_pair.delete()
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'Key - {key_pair_name} has been removed')
            return response

    def get_latest_ami(self):
        """
        Get latest target AMI
        :return: ami object
        """
        try:
            response = self.client.describe_images(
                Filters=[
                    {'Name': 'name', 'Values': ['amzn-ami-hvm*']},
                    {'Name': 'architecture', 'Values': ['x86_64']},
                    {'Name': 'root-device-type', 'Values': ['ebs']},
                    {'Name': 'hypervisor', 'Values': ['xen']},
                    {'Name': 'state', 'Values': ['available']},
                    {'Name': 'virtualization-type', 'Values': ['hvm']},
                    {'Name': 'block-device-mapping.volume-type',
                        'Values': ['gp2']},
                    {'Name': 'owner-alias', 'Values': ['amazon']},
                ],
                Owners=[
                    'amazon',
                ],
                DryRun=False
            )

            images = sorted(response['Images'],
                            key=lambda x: x['CreationDate'],
                            reverse=True)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'Found {len(response)} images, returning the latest one')
            return images[0]

    def get_instance_detail(self, server_name: str = None):
        """
        Describe EC2 instance
        :param server_name: Optional: Server name
        :return: Response from describe_instances request
        """
        if server_name is None:
            server_name = self.config['name']

        try:
            response = self.client.describe_instances(
                Filters=[
                    {
                        'Name': 'tag:ServerName',
                        'Values': [
                            server_name,
                        ]
                    },
                ],
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(
                f'{len(response["Reservations"][0]["Instances"])} EC2 instance(s) found to match the name - {server_name}')
            return response
