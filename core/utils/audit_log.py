import DjangoSetup
from core.models import db_audit_trail

def audit_log(activity_code,activity_desc,severity,user,param1=None,param2=None,param3=None):
    db_audit_trail.log(act_code=activity_code,act_desc=activity_desc,severity=severity,user=user,param_1=param1,param_2=param2,param_3=param3)