from .base_service import BaseService
from aws.utils import parameter_check


class ConfigService(BaseService):
    """
    Config Class, including AWS Config related operations
    """
    def __init__(self, service_name='config', app_config="app_config.ini"):
        """
        Initiate AWS service class
        :param service_name: AWS service name
        :param app_config: App configuration file name
        """
        BaseService.__init__(self, service_name, app_config)

    def get_conformance_pack_details(
            self,
            conformance_pack_name: str,
            config_rule_names: list = None,
            compliance_type: str = None,
            resource_type: str = None,
            resource_ids: list = None) -> list:
        """
        Get conformance pack information
        :return: A list of config rules in dict
        """
        # Build filter
        filters = {}
        if config_rule_names is not None:
            config_rule_names = parameter_check(config_rule_names, list)
            filters["ConfigRuleNames"] = config_rule_names

        if compliance_type is not None:
            compliance_type = parameter_check(compliance_type, str, value_range={"INSUFFICIENT_DATA", "NON_COMPLIANT", "COMPLIANT"})
            filters["ComplianceType"] = compliance_type

        if resource_type is not None:
            resource_type = parameter_check(resource_type, str)
            filters["ResourceType"] = resource_type

        if resource_ids is not None:
            resource_ids = parameter_check(resource_ids, list)
            filters["ResourceIds"] = resource_ids

        try:
            # HomeMade Paginator
            all_response = []
            next_token = ""
            while next_token is not None:
                response = self.client.get_conformance_pack_compliance_details(
                    ConformancePackName=conformance_pack_name,
                    Filters=filters,
                    Limit=100,
                    NextToken=next_token
                )
                if "NextToken" in response:
                    next_token = response["NextToken"]
                else:
                    next_token = None
                all_response.extend(response["ConformancePackRuleEvaluationResults"])
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return_response = []
            for item in all_response:
                rule = {
                    "ComplianceType"    : item["ComplianceType"],
                    "ConfigRuleName"    : item["EvaluationResultIdentifier"]["EvaluationResultQualifier"]["ConfigRuleName"],
                    "ResourceType"      : item["EvaluationResultIdentifier"]["EvaluationResultQualifier"]["ResourceType"],
                    "ResourceId"        : item["EvaluationResultIdentifier"]["EvaluationResultQualifier"]["ResourceId"],
                    "ResultRecordedTime": item["ResultRecordedTime"]
                }
                return_response.append(rule)

            return return_response

    def describe_compliance_by_config_rule(self, config_rule_names: str) -> list:
        """
        Get a list of findings by config rules
        :param config_rule_names: A config rule name
        :return: A list of findings
        """
        # Build filter
        if config_rule_names is not None:
            config_rule_names = parameter_check(config_rule_names, str)

        try:
            # HomeMade Paginator
            all_response = []
            next_token = ""
            while next_token is not None:
                response = self.client.get_compliance_details_by_config_rule(
                    ConfigRuleName=config_rule_names,
                    NextToken=next_token
                )
                if "NextToken" in response:
                    next_token = response["NextToken"]
                else:
                    next_token = None
                all_response.extend(response["EvaluationResults"])
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return_response = []
            for item in all_response:
                rule = {
                    "ComplianceType"    : item["ComplianceType"],
                    "ConfigRuleName"    : item["EvaluationResultIdentifier"]["EvaluationResultQualifier"]["ConfigRuleName"],
                    "ResourceType"      : item["EvaluationResultIdentifier"]["EvaluationResultQualifier"]["ResourceType"],
                    "ResourceId"        : item["EvaluationResultIdentifier"]["EvaluationResultQualifier"]["ResourceId"],
                    "ResultRecordedTime": item["ResultRecordedTime"]
                }
                return_response.append(rule)

            return return_response
