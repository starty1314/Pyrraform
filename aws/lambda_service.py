from .base_service import BaseService
import zipfile
import os
import configparser


class Lambda_service(BaseService):
    """
    Lambda class, to help with Lambda related operations
    """
    def __init__(self, service_name='lambda', app_config="app_config.ini"):
        """
        Initiate AWS service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        BaseService.__init__(self, service_name, app_config)

    def create_lambda(self, iam_role_arn: str, kms_key_arn: str, environment: str, tags: dict):
        """
        Create a lambda function
        :param iam_role_arn: IAM role ARN that needs to be attached to lambda
        :param kms_key_arn: KSM key ARN
        :param environment: Preferred running environment, such as Python
        :param tags: tags
        :return: Response from create lambda request
        """
        # zip dependencies
        def zip_lib(libraries, zf):
            # zf is zipfile handle
            lib_path = os.path.join(self.cwd, 'venv\Lib\site-packages')
            for lib in libraries:
                lib_full_path = f'{lib_path}\\{lib}'
                if os.path.isfile(lib_full_path):
                    zf.write(lib_full_path, os.path.basename(lib_full_path))
                elif os.path.isdir(lib_full_path):
                    for root, dirs, files in os.walk(lib_full_path):
                        for file in files:
                            zf.write(os.path.join(root, file), f'{lib}\\{file}')

        # Get current working directory
        cwd = self.cwd
        project_name = self.project_name
        lambda_file = os.path.join(cwd, f'{project_name}.py')

        setup_config = configparser.ConfigParser()
        setup_config.read(self.config_file)

        libs = list(dict(setup_config.items('lib'))['libs'].split(","))

        zip_file = os.path.join(cwd, f'{self.config["function_name"]}.zip')

        # Create lambda package
        zf = zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED)
        try:
            print(f'Creating {self.config["function_name"]}.zip...')
            zf.write(lambda_file, os.path.basename(lambda_file))
            zip_lib(libs, zf)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'{self.config["function_name"]}.zip is created')
        finally:
            print('Closing zipfile...')
            null = zf.close()

        # Create Lambda functions
        try:
            response = self.client.create_function(
                FunctionName=self.config['function_name'],
                Runtime=self.config['runtime'],
                Role=iam_role_arn,
                Handler=f'{self.config["function_name"]}.lambda_handler',
                Code={
                    'ZipFile': open(zip_file, 'rb').read()
                },
                Description=self.config['description'],
                Timeout=60,
                MemorySize=128,
                Publish=False,
                Environment=environment,
                KMSKeyArn=kms_key_arn,
                Tags=tags
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return response

    def add_trigger(self, events_rule_arn: str):
        """
        Create a trigger for lambda job
        :param events_rule_arn: EventsBridge Rule ARN
        :return: Response from lambda add_permission request
        """
        response = self.client.add_permission(
            FunctionName=self.config['function_name'],
            StatementId=self.config['statement_id'],
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=events_rule_arn
        )
        return response

    def delete_lambda(self):
        """
        Delete lambda function
        :return: Response from lambda delete request
        """
        try:
            response = self.client.delete_function(FunctionName=self.config['function_name'])
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'Lambda function {self.config["function_name"]} has been removed')
            return response
