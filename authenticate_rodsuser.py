import sys, os
from irods.session import iRODSSession 

service_acct_env_file=os.path.expanduser("~/.irods/irods_environment.json")
c = None

with iRODSSession(irods_env_file = service_acct_env_file) as s1:

  with iRODSSession(user=sys.argv[1],
                    password=sys.argv[2],
                    port=s1.port, host=s1.host, zone=s1.zone) as s2:
 
    try:
      c = s2.collections.get('/{}/home/{}'.format(s2.zone,s2.username))
    except: pass

if c is None:
  raise SystemExit(1)
