from .base_service import BaseService
from aws import sts
from aws import organizations
from datetime import datetime
from pytz import timezone
from aws.exceptions import NotFoundException, NotSupportException
from aws.utils import logger


class SecurityHub(BaseService):
    """
    SecurityHub Class, include SecurityHub related operations
    """
    def __init__(self, service_name: str = 'securityhub', app_config: str = "app_config.ini"):
        BaseService.__init__(self, service_name, app_config)

    def get_members_info(self) -> list:
        """
        Get all the monitored accounts in Securityhub
        :return: A list of dict
                {
                    "accountId": "694464613",
                    "type"     : "member",
                    "alias"    : "Prod"
                }
        """
        org = organizations.Organizations()

        response = []

        try:
            # Get all Security Hub members
            members = self.client.list_members()["Members"]

            # Reconstruct member information
            for member in members:
                account_alias = org.get_account_info(member["AccountId"])["Account"]["Name"]
                temp = {
                    "accountId": member["AccountId"],
                    "type"     : "member",
                    "alias"    : account_alias
                }
                response.append(temp)

            if len(members) != 0:
                # Adding master account information
                master_account = {
                    "accountId": members[0]["MasterId"],
                    "type"     : "master",
                    "alias"    : "SIEM"
                }
                response.append(master_account)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return response

    def get_status(self) -> bool:
        """
        Get Security Hub status
        :return: True or False
        """
        try:
            response = self.client.get_enabled_standards()
        except self.client.exceptions.InvalidAccessException:
            return False
        else:
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return True

    def get_benchmark_by_name(self, benchmark_name: str):
        """
        Get a benchmark by name
        :param benchmark_name:
        :return: The specific benchmark
        """
        try:
            self._check_supported_benchmark(benchmark_name)

            benchmarks = self.get_available_benchmarks()

            benchmark = self._json_filter("Name", benchmark_name, benchmarks)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return benchmark

    def get_enabled_benchmark_by_name(self, benchmark_name: str):
        """
        Get an enabled benchmark by name
        :param benchmark_name: Benchmark name
        :return: The specific enabled benchmark by name or False if benchmark not found
        """
        try:
            self._check_supported_benchmark(benchmark_name)

            benchmarks = self.get_enabled_benchmarks()
            benchmark = self.get_benchmark_by_name(benchmark_name)
            benchmark = self._json_filter("StandardsArn", benchmark["StandardsArn"], benchmarks)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return benchmark

    def get_available_benchmarks(self) -> list:
        """
        Get a list of standards that AWS supports
        :return: A list of standards that AWS supports
        """
        try:
            response = self.client.describe_standards()
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return response["Standards"]

    def get_enabled_benchmarks(self):
        """
        Get a list of enabled benchmarks
        :return: A list of enabled benchmarks
        """
        try:
            response = self.client.get_enabled_standards()

        except self.client.exceptions.InvalidAccessException:
            return "Security Hub is disabled"
        else:
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return response["StandardsSubscriptions"]

    def enable_benchmark(self, benchmark_name: str) -> bool:
        """
        Enable one benchmark
        :param benchmark_name: Benchmark Name
        :return: True or raise exception
        """
        try:
            self._check_supported_benchmark(benchmark_name)

            benchmark = self.get_benchmark_by_name(benchmark_name)

            response = self.client.batch_enable_standards(
                StandardsSubscriptionRequests=[
                    {
                        'StandardsArn': benchmark['StandardsArn']
                    }
                ]
            )
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return True

    def enable_benchmarks(self, benchmark_names: list) -> bool:
        """
        Enable a list of benchmarks by name
        :param benchmark_names: A list of benchmark names
        :return: True or raise exception
        """
        try:
            for benchmark_name in benchmark_names:
                self.enable_benchmark(benchmark_name)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return True

    def disable_benchmark(self, benchmark_name: str):
        """
        Disable a benchmark by name
        :param benchmark_name: Benchmark Name
        :return: True or raise exception
        """
        try:
            self._check_supported_benchmark(benchmark_name)

            benchmark_name = self.get_enabled_benchmark_by_name(benchmark_name)

            if benchmark_name:
                response = self.client.batch_disable_standards(
                    StandardsSubscriptionArns=[
                        benchmark_name["StandardsSubscriptionArn"],
                    ]
                )
            else:
                return "Benchmark is not enabled"
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return True

    def disable_benchmarks(self, benchmark_names: list) -> bool:
        """
        Disable a list of enabled benchmarks by name
        :param benchmark_names: A list of benchmark names
        :return: True or raise exceptions
        """
        try:
            for benchmark_name in benchmark_names:
                self.disable_benchmark(benchmark_name)
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return True

    def get_settings_by_benchmark(self, benchmark_name: str) -> list:
        """
        Get a list of settings in a benchmark
        :param benchmark_name: Benchmark Name
        :return: A list of settings
        """
        try:
            self._check_supported_benchmark(benchmark_name)

            benchmark = self.get_enabled_benchmark_by_name(benchmark_name)

            for bm in benchmark:
                response = self.client.describe_standards_controls(StandardsSubscriptionArn=bm["StandardsSubscriptionArn"])

        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return response

    def get_findings_by_benchmark(self, benchmark_name: str, aws_account_id=None) -> list:
        """
        Get findings in Security Hub by AWS account ID and benchmark name
        :return: A list of findings
        """
        try:
            self._check_supported_benchmark(benchmark_name)

            benchmark = self.get_enabled_benchmark_by_name(benchmark_name)
            if aws_account_id is None:
                s = sts.STS()
                aws_account_id = s.get_account_id()

            paginator = self.client.get_paginator('get_findings')
            response_iterator = paginator.paginate(
                Filters={
                    'AwsAccountId': [
                        {
                            'Value': aws_account_id,
                            'Comparison': 'EQUALS'
                        },
                    ],
                    'ProductFields': [
                        {
                            'Key': 'StandardsGuideArn',
                            'Value': benchmark["StandardsArn"],
                            'Comparison': 'EQUALS'
                        }
                    ],
                },
            )

            data = []
            for i in response_iterator:
                data += i["Findings"]

            now_time = datetime.now(timezone('US/Eastern'))
            report_time = now_time.strftime('%Y-%m-%d %H:%M:%S')

            findings = []
            for finding in data:
                if "ControlId" in finding["ProductFields"]:
                    item = {
                        "severity"       : finding["Severity"]["Label"],
                        "status"         : finding["Compliance"]["Status"],
                        "setting_id"     : finding["ProductFields"]["ControlId"],
                        "title"          : finding["Title"].replace(finding["Title"].split(" ")[0], "").strip(),
                        "resources"      : str(finding["Resources"]),
                        "benchmark"      : benchmark_name,
                        "aws_account_id" : aws_account_id,
                        "dt"             : report_time
                    }
                    findings.append(item)
                elif "RuleId" in finding["ProductFields"]:
                    item = {
                        "severity"      : finding["Severity"]["Label"],
                        "status"        : finding["Compliance"]["Status"],
                        "setting_id"    : finding["ProductFields"]["RuleId"],
                        "title"         : finding["Title"].replace(finding["Title"].split(" ")[0], "").strip(),
                        "resources"     : str(finding["Resources"]),
                        "benchmark"     : benchmark_name,
                        "aws_account_id": aws_account_id,
                        "dt"            : report_time
                    }
                    findings.append(item)
            sorted_findings = self._sort_settings(findings, benchmark_name)

        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return sorted_findings

    def get_all_findings(self) -> list:
        """
        Get all findings in Security Hub
        :return: A list of findings
        """
        try:
            paginator = self.client.get_paginator('get_findings')
            response_iterator = paginator.paginate()
            data = []
            for i in response_iterator:
                data += i["Findings"]
        except Exception as e:
            self.logger.exception("")
            raise e
        else:
            return data

    @staticmethod
    def _json_filter(key: str, value: str, list_object: list) -> list:
        """
        Filter JSON list by value of key
        :param key: A key in the object
        :param value: The value that is wanted
        :param list_object: A list of objects
        :return: A list of objects that matches the key/value condition
        """
        try:
            response = [x for x in list_object if x[key] == value]
        except Exception as e:
            logger().exception("")
            raise e
        else:
            if len(response) == 0:
                raise NotFoundException("Benchmark not found")
            return response[0]

    @staticmethod
    def _sort_settings(findings: list, benchmark_name: str) -> list:
        """
        Sort a list of settings
        :param benchmark_name: Benchmark Name
        :param findings: A list of settings
        :return: A sorted list of settings
        """
        # Get a list of IDs
        id_findings = []
        for finding in findings:
            id_findings.append(finding["setting_id"])

        # Sort IDs
        sorted_id_findings = []
        id_list = []
        temp = []

        if benchmark_name == "CIS AWS Foundations Benchmark v1.2.0":
            for item in id_findings:
                id_list.append(([int(i) for i in item.split(".")]))
                temp.append(int(item.split(".")[0]))

            first_id_list = list(dict.fromkeys(temp))
            first_id_list.sort()

            for first_id in first_id_list:
                temp = []
                for item in id_list:
                    if item[0] == first_id:
                        temp.append(item)
                temp.sort(key=(lambda e: e[1]))
                temp = [f'{i[0]}.{i[1]}' for i in temp]
                sorted_id_findings += temp

        if benchmark_name == "AWS Foundational Security Best Practices v1.0.0":
            for item in id_findings:
                id_list.append(([i for i in item.split(".")]))
                temp.append(item.split(".")[0])

            first_id_list = list(dict.fromkeys(temp))
            first_id_list.sort()

            for first_id in first_id_list:
                temp = []
                for item in id_list:
                    if item[0] == first_id:
                        temp.append(item)
                temp.sort(key=(lambda e: int(e[1])))
                temp = [f'{i[0]}.{i[1]}' for i in temp]
                sorted_id_findings += temp

        # Remove duplicates
        sorted_id_findings = list(dict.fromkeys(sorted_id_findings))

        sorted_findings = []
        for sorted_id_finding in sorted_id_findings:
            for finding in findings:
                if sorted_id_finding == finding["setting_id"]:
                    sorted_findings.append(finding)

        return sorted_findings

    def _check_supported_benchmark(self, benchmark_name: str) -> bool:
        """
        Check if the benchmark is supported
        :param benchmark_name: Benchmark Name
        :return: True if supported, Raise exception if not supported
        """
        supported_benchmarks = (self.config["supported_benchmarks"]).split(";")

        if benchmark_name in supported_benchmarks:
            return True
        else:
            raise NotSupportException(f'Benchmark "{benchmark_name}" is not supported.')

    def get_benchmark_stats(self, data) -> dict:
        """
        Return benchmark stats in dict format
        :param data: Data in dataframe
        :return: Benchmark stats
        """
        return {
            "total_settings"            : self._get_total_settings(data),
            "total_failed"              : self._get_total_failed(data),
            "total_not_available"       : self._get_total_not_available(data),
            "total_passed"              : self._get_total_passed(data),
            "total_unique_settings"     : self._get_total_unique_settings(data)[0],
            "total_unique_failed"       : self._get_total_unique_failed(data),
            "total_unique_not_available": self._get_total_unique_not_available(data),
            "total_unique_passed"       : self._get_total_unique_passed(data)[0],
            "compliant_score"           : self.get_compliant_score(data),
            "dt"                        : data.iloc[0]["dt"],
            "aws_account_id"            : data.iloc[0]["aws_account_id"],
            "benchmark"                 : data.iloc[0]["benchmark"]
        }

    @staticmethod
    def _get_total_settings(data) -> int:
        """
        Get the number of total settings
        :param data: All findings data in Dataframe format
        :return: The number of total settings
        """
        total_settings = data.shape[0]
        return total_settings

    @staticmethod
    def _get_total_failed(data) -> int:
        """
        Get the number of total failed settings
        :param data: All findings data in Dataframe format
        :return: The number of total failed settings
        """
        total_failed = data.loc[data['status'] == "FAILED"].shape[0]
        return total_failed

    @staticmethod
    def _get_total_not_available(data) -> int:
        """
        Get the number of total "NOT Available" settings
        :param data: All findings data in Dataframe format
        :return: The number of total "NOT Available" settings
        """
        total_not_available = data.loc[data['status'] == "NOT_AVAILABLE"].shape[0]
        return total_not_available

    @staticmethod
    def _get_total_passed(data) -> int:
        """
        Get the number of total passed settings
        :param data: All findings data in Dataframe format
        :return: The number of total passed settings
        """
        total_passed = data.loc[data['status'] == "PASSED"].shape[0]
        return total_passed

    def _get_total_unique_settings(self, data):
        """
        Get the number of total unique settings
        :param data: All findings data in Dataframe format
        :return: The number of total unique settings, and a list of unique settings
        """
        total_unique_settings = data.setting_id.unique()
        return len(total_unique_settings), total_unique_settings

    def _get_total_unique_failed(self, data) -> int:
        """
        Get the number of total unique failed settings
        :param data: All findings data in Dataframe format
        :return: The number of total unique failed settings
        """
        list_of_settings = data.setting_id.unique()
        total_unique_failed = 0
        settings = data.groupby("setting_id")
        for setting in list_of_settings:
            settings_in_group = settings.get_group(setting)
            if settings_in_group[settings_in_group["status"] == "PASSED"].shape[0] == 0:
                if settings_in_group[settings_in_group["status"] == "FAILED"].shape[0] == 0:
                    if settings_in_group[settings_in_group["status"] == "NOT_AVAILABLE"].shape[0] >= 1:
                        total_unique_failed += 1

        return total_unique_failed

    def _get_total_unique_not_available(self, data) -> int:
        """
        Get the number of total unique, but "NOT-Available" settings
        :param data: All findings data in Dataframe format
        :return: The number of total unique, but "NOT-Available" settings
        """
        list_of_settings = data.setting_id.unique()
        total_unique_not_available = 0
        settings = data.groupby("setting_id")
        for setting in list_of_settings:
            settings_in_group = settings.get_group(setting)
            if settings_in_group[settings_in_group["status"] == "PASSED"].shape[0] == 0:
                if settings_in_group[settings_in_group["status"] == "NOT_AVAILABLE"].shape[0] == 0:
                    if settings_in_group[settings_in_group["status"] == "FAILED"].shape[0] >= 1:
                        total_unique_not_available += 1

        return total_unique_not_available

    def _get_total_unique_passed(self, data):
        """
        Get the number of total unique passed settings
        :param data: All findings data in Dataframe format
        :return: The number of total unique passed settings, and a list of passed settings' ID
        """
        list_of_settings = data.setting_id.unique()
        settings_passed = []
        settings = data.groupby("setting_id")
        for setting in list_of_settings:
            settings_in_group = settings.get_group(setting)
            if settings_in_group[settings_in_group["status"] == "FAILED"].shape[0] == 0:
                if settings_in_group[settings_in_group["status"] == "NOT_AVAILABLE"].shape[0] == 0:
                    if settings_in_group[settings_in_group["status"] == "PASSED"].shape[0] >= 1:
                        settings_passed.append(setting)

        return len(settings_passed), settings_passed

    def get_compliant_score(self, data) -> float:
        """
        Calculate the compliance score percentage
        Compliance score = total unique passed settings / total unique settings
        :param data: All findings data in Dataframe format
        :return: The compliance score percentage in float
        """
        compliant_score = float(self._get_total_unique_passed(data)[0] / self._get_total_unique_settings(data)[0])
        return round(compliant_score, 3)

    def get_compliance_report(self, data):
        """
        Get benchmark's compliance report
        If there is only one check of the setting failed, then the entire setting fails
        :param data: The finding data, all checks(One setting has multiple checks)
        :return: Report data in dataframe
        """
        n, list_of_passed_settings = self._get_total_unique_passed(data)

        report_columns = ["setting_id", "title", "benchmark", "aws_account_id", "dt"]
        report = data[report_columns].drop_duplicates()

        # Add status column to the data, default value is FAILED
        report["status"] = "FAILED"

        # Update all the passed settings' status to PASSED
        report.loc[report.setting_id.isin(list_of_passed_settings), "status"] = "PASSED"
        report = report.rename(columns={"setting_id": "ID", "title": "Setting", "benchmark": "Benchmark", "aws_account_id": "AWS Account ID", "dt": "Report time", "status": "Status"})
        columns = ["Status", "ID", "Setting", "AWS Account ID", "Report time", "Benchmark"]
        return report[columns]
