
from __future__ import print_function
import os, sys
import irods
from irods.session import iRODSSession
from os.path import expanduser, curdir, sep ,abspath, join
from sys import argv as orig_argv, stderr, stdout, exit
from getopt import GetoptError, getopt
import json

# admin_as_rodsuser.py [-z tempZone] -U rods -P rods -u alice -p /phys/path/to/file -m mdmanifest_file.json

try:
  opt, argv = getopt( 
         orig_argv[1:], 'j:J:p:r:c:r:U:P:u:z:m:H:I:v:' # 'E:' <-- (disabling for now)
  )
except GetoptError:
  print ( '''Usage: %s [options]
  where options are:
  \t -j json-config ( read from admin credentials file in .JSON )
  \t -J password ( write to admin credentials file in .JSON )
  \t -h help  (print this usage statement)
  \t -H hostname  (machine with target iRODS server for our connect)
  \t -I portN (integer - iRODS server port # to connect to )
  \t -p physical path (file to register into iRODS)
  \t -r <resc_name>   (resource from which to register)
  \t -U <admin_user_name>  (usually 'rods')
  \t -P <admin_user_pw>
  \t -E <admin_user_environment_file.json>
  \t -u <client_user> (client user on behalf of which we are working)
  \t -z <iRODS_zone>
  \t -m <md_manifest> (.JSON file - destination collection & metadata ops.)
  \t -v N (set verbosity level)
  ''' % (orig_argv[0],) , file = stderr )
  exit(1)

argv.insert(0,orig_argv[0])
options = {}
options.update( opt )

#-=-=-=-=-=- Change admin password file

if options.get('-J','') and (options.get('-u') is None): 
  new_pw = options.get('-P','')
  new_user =  options.get('-U',None)
  if type(new_user) is str and len(new_user) == 0:
    print ("ERROR: need admin username of >= 1 character", file = sys.stderr)
    raise SystemExit(1)
  try:
    with open (options['-J']) as f:
      admin_credentials =  json.load(f)
      admin_credentials ['admin_password'] = new_pw if len(new_pw) else None
      if new_user is not None:
        admin_credentials ['admin_username'] = new_user
    with open (options['-J'],'w') as f:
      json.dump(admin_credentials,f,indent=2)
  except: 
    print ("Failed to change admin credentials.(For reasons.)",file=sys.stderr)
    exit(3)
  finally:
    exit(0)

admin_username = options.get('-U')
admin_password = options.get('-P')   

if not(admin_username) or not(admin_username) :
  default_admin_creds_file = expanduser('~/compute/admin_as_user.json')
  jsonConfigFile = options.get('-j',default_admin_creds_file)
  with open(jsonConfigFile) as f:
    jsonConfig = json.load(f)
    admin_username = str(jsonConfig.get('admin_username'))
    admin_password = str(jsonConfig.get('admin_password'))

assert ( admin_username and admin_password )

irods_server_host = options.get('-H','localhost')
irods_server_port = int(options.get('-I','1247'))

user = options.get('-u','')            # - client user
zone = options.get('-z', 'tempZone' )  # - iRODS zone for compute-to-data ex.

env_file = options.get( '-E', '' )
if env_file:
  print('"-E" is presently a disabled feature', file=sys.stderr)
  raise SystemExit(2)

phyP = options.get('-p' ,'')
rescN = options.get('-r','')

verbosity = int( options.get('-v','0') )
if verbosity >= 2:
  _input = None
  try:    _input = raw_input        ## Python 2
  except NameError: _input = input  ## Python 3

# =-=-=-=-=

options_for_session = { 'client_user': user,
                        'irods_env_file' :  env_file,
                        'password': admin_password
                      }
session = None

for k in ('client_user', 
          'password',
          'irods_env_file' ):
  if not options_for_session[k]: del options_for_session[k] 

if not session:
  if not user:
    session = iRODSSession ( host= irods_server_host , port = irods_server_port,
                             user = admin_username , password = admin_password,
                             zone = zone, *options_for_session )
  else:
    session = iRODSSession( # host='localhost', port=1247,
		host = irods_server_host , port = irods_server_port , 
                user = admin_username , password = admin_password ,
                zone = zone , client_user = user )
                          # user = admin_user , password = admin_pass,
                          # zone = zone, client_user = user

if session:
  test_user = (user or admin_username)
  collName = '/tempZone/home/{}'.format(test_user)
  collObj = session.collections.get( collName  )
  dataObjs = collObj.data_objects
  print( "{!r}".format(dataObjs) , file = stderr )

if verbosity >= 1:
  print( "\tActing for user '{}'".format(session.username) , file = stderr )

#==============================================

md_manifest_file = options['-m']

if not phyP: exit(1)

mdManifest = {}
with open(md_manifest_file,'r') as f:
  mdManifest = json.load(f)

register_opts = { 'rescName': 'demoResc' }
parentCollName =  mdManifest["parentIrodsTargetPath"]
parentColl = None

try:
  parentColl = session.collections.get(parentCollName)
except Exception as e:
  print(  repr(e) + "\n -- trying create" ,file=stderr)
  parentColl = session.collections.create(parentCollName)

if not(rescN):
  del register_opts['rescName'] 
else:
  register_opts['rescName'] = rescN

assert os.path.isfile( phyP )

data_object_name = os.path.basename (phyP)
logP = parentColl.path + "/" + data_object_name
  
answer = 'y'

if verbosity >= 1:
  print ('username = "{}" ' .format( session.username), file = stderr)
  print ('phyP     = "{}" ' .format( phyP), file = stderr )
  print ('logP     = "{}" ' .format( logP), file = stderr )
  print ('rescN    = "{}" ' .format( rescN), file = stderr )
  print ('opts     = "{!r}" ' .format( register_opts), file = stderr )
  if verbosity >= 2:
    assert _input is not None
    answer = _input('proceed (y/n) -> ')

if answer.upper().strip() == 'Y':
  print ( '** Registering **' , file = stderr )
  session.data_objects.register ( phyP , logP , **register_opts )

