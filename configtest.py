# from aws.athena import Athena
#
#
# cs = Athena()
#
# pack = cs.
#
# print(pack)


from aws.config_service import ConfigService
from dataops import DataOps

cs = ConfigService()
do = DataOps()

# result = cs.get_conformance_pack_details("cis13-level2")
#
# result = do.export_csv(result, '.\\test.csv')

result = cs.client.describe_compliance_by_config_rule(ConfigRuleNames=['cis13-level2'])

print(result)