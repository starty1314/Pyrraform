from dataops import DataOps
from dbops import DBOps
from emailops import EmailOps
from aws.securityhub import SecurityHub
from aws.rds import RDS
from aws.ses import SES
from model import CISAWSReport, CISAWSReportStats
from datetime import datetime
from pytz import timezone
import platform
from utils import option_loader


# Business logic resides here
def lambda_handler(event, context):
    APP_CONFIG = "app_config.ini"

    print("starting")
    # Initialize aws services
    securityhub = SecurityHub(app_config=APP_CONFIG)
    rds = RDS(app_config=APP_CONFIG)
    data_ops = DataOps()

    print("starting DB")
    # Loading database connection info
    db_server        = option_loader(APP_CONFIG, "db", "db_server")
    db_name          = option_loader(APP_CONFIG, "db", "db_name")
    user_name        = option_loader(APP_CONFIG, "db", "user_name")
    port             = option_loader(APP_CONFIG, "db", "port")
    db_instance_type = option_loader(APP_CONFIG, "db", "db_instance_type")
    # password         = data_ops.option_loader(APP_CONFIG, "db", "password")
    password         = rds.get_temp_password(db_server, port, user_name)
    ca_path          = option_loader(APP_CONFIG, 'db', "ca_path")

    # # Initiate DB connection
    # db_ops = DBOps(db_server=db_server,
    #                db_name=db_name,
    #                user_name=user_name,
    #                password=password,
    #                port=port,
    #                ca_path=ca_path,
    #                db_instance_type=db_instance_type
    #                )
    print("starting loading options from configuration")
    benchmark_name = option_loader(APP_CONFIG, "app", "benchmark")
    # time = datetime.now().strftime("%Y%m%d%H%M%S")
    now_time  = datetime.now(timezone('US/Eastern'))
    report_time = now_time.strftime("%Y%m%d%H%M%S")
    report_time_subject = now_time.strftime("%Y-%m-%d")
    report_time_body = now_time.strftime("%m/%Y/%d %H:%M:%S")

    print("Getting securityhub memebers")
    # List of aggregated security hub accounts
    members = securityhub.get_members_info()
    print("starting finding calculation")
    print(members)
    members = [{
        "accountId": "123456789012",
        "type"     : "member",
        "alias"    : "lab_account",
    }]
    for member in members:
        findings = securityhub.get_findings_by_benchmark(benchmark_name, aws_account_id=member["accountId"])
        print(findings)
        if findings:
            if platform.system() == "Windows":
                finding_file = f'BAFS_v1.0_Findings_{report_time}_{member["accountId"]}_{member["alias"]}.xlsx'
                report_file  = f'BAFS_v1.0_Report_{report_time}_{member["accountId"]}_{member["alias"]}.xlsx'
            else:
                finding_file = f'/tmp/BAFS_v1.0_Findings_{report_time}_{member["accountId"]}_{member["alias"]}.xlsx'
                report_file  = f'/tmp/BAFS_v1.0_Report_{report_time}_{member["accountId"]}_{member["alias"]}.xlsx'

            # Export all findings to excel
            data_ops.export_excel(findings, finding_file)

            # Converting data into various format for manipulation
            data_in_dataframe = data_ops.convert_to_dataframe(findings)

            # Get statistic data from all findings and convert to a report
            stats_in_dict = securityhub.get_benchmark_stats(data_in_dataframe)
            stats_in_dict["account_alias"] = member["alias"]
            stats_in_df = data_ops.convert_to_dataframe([stats_in_dict.copy()])

            # Get report specific data in dataframe
            report = securityhub.get_compliance_report(data_in_dataframe)

            columns = ["Status", "ID", "Setting"]
            report = report[columns]

            # Export report specific data to excel by leveraging dataframe builtin function
            report.to_excel(report_file, sheet_name="Benchmark", index=False)

            # Adding header to HTML report
            a = report.columns.values.tolist()
            report = report.values.tolist()
            report.insert(0, a)

            # # Send data in email
            # email_ops = SES()
            email_ops = EmailOps()
            email_ops["Subject"]    = f"{benchmark_name} Report for {report_time_subject}"
            # email_ops["From"]       = utils.option_loader(APP_CONFIG, "email", "from")
            # email_ops["To"]         = [utils.option_loader(APP_CONFIG, "email", "to")]
            email_ops["To"]       = "rohua.h@gmail.com"
            email_ops["From"]     = "70c6f159ccb41f"
            email_ops["SMTP"]     = "smtp.mailtrap.io:587"
            email_ops["Password"] = "2ec6b1d7bad07b"
            email_ops["Attachment"] = [finding_file, report_file]

            email_ops.add_heading(f"AWS Account           : {member['alias']}")
            email_ops.add_heading(f"Report Time           : {report_time_body}")
            email_ops.add_heading(f"Total settings        : {stats_in_dict['total_settings']}")
            email_ops.add_heading(f"Total Unique settings : {stats_in_dict['total_unique_settings']}")
            email_ops.add_heading(f"Compliance Score      : {stats_in_dict['compliant_score']*100}%")
            email_ops.add_heading(f"Benchmark             : {stats_in_dict['benchmark']}")
            email_ops.add_ending("Thanks,")
            email_ops.add_ending("John Doe")
            print("sending emails")
            email_ops.send_email(report)
            print("email sent")

            # # Create the table if not exist
            # CISAWSReport.check_table_existence(db_ops.engine)
            # CISAWSReportStats.check_table_existence(db_ops.engine)
            # print("saving data")
            # db_ops.insert_dataframe(CISAWSReport, data_in_dataframe)
            # db_ops.insert_dataframe(CISAWSReportStats, stats_in_df)
            # print("data saves")

    print("done")


if __name__ == '__main__':
    lambda_handler(1, 1)
