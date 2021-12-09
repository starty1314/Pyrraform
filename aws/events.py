from .base_service import BaseService


class Events(BaseService):
    """
    CloudWatch Events class, to help with events related operations
    """
    def __init__(self, service_name='events', app_config="app_config.ini"):
        """
        Initiate AWS service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        BaseService.__init__(self, service_name, app_config)

    def create_rule(self, iam_role_arn: str, tags: dict):
        """
        Create rule/schedule in eventBridge
        :param iam_role_arn: IAM Role
        :param tags: tags
        :return: Response from put_rule request
        """
        try:
            response = self.client.put_rule(
                Name=self.config['name'],
                ScheduleExpression=self.config['schedule_expression'],
                State='ENABLED',
                Description=self.config['description'],
                RoleArn=iam_role_arn,
                Tags=tags
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'Event Rule {self.config["name"]} has been created')
            return response

    def delete_rule(self):
        """
        Delete rule/schedule in eventBridge
        :return: Response from delete_rule request
        """
        try:
            response = self.client.delete_rule(Name=self.config['name'])
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'EventBridge rule {self.config["name"]} has been removed')
            return response

    def remove_targets(self):
        """
        remove the specified targets from a specified rule in eventBridge
        :return: Response from remove_targets request
        """
        try:
            response = self.client.remove_targets(Rule=self.config['name'], Ids=[self.config['name']])
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            print(f'Target has been removed from EventBridge rule {self.config["name"]}')
            return response

    def attach_to_target(self, target_arn: str):
        """
        Attach EventBridge rule to target service
        :param target_arn: EventBridge rule name, target id, target Arn
        :return: Response from put_targets request
        """
        try:
            response = self.client.put_targets(
                Rule=self.config['name'],
                Targets=[{
                    'Id': self.config['name'],
                    'Arn': target_arn
                }]
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            list_arn = target_arn.split(":")
            service_name = list_arn[-1]
            service = list_arn[2]
            print(f'Attached rule {self.config["name"]} to {service} - {service_name}')
            return response
