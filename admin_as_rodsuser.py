
from __future__ import print_function
import os, sys
import irods
from irods.session import iRODSSession
from os.path import expanduser, curdir, sep ,abspath, join
from sys import argv as orig_argv, stderr, stdout, exit
from getopt import GetoptError, getopt
import json

# admin_as_rodsuser.py -p /phys/path/to/file -m mdmanifest_file.json -u alice

try:
  opt, argv = getopt( orig_argv[1:], 'p:r:r:u:m:v:e:' )
except GetoptError:
  print ( '''Usage: %s [options]
  where options are:
  \t -h help  (print this usage statement)
  \t -p physical path (file to register into iRODS)
  \t -r <resc_name>   (resource from which to register)
  \t -e <admin user irods_environment.json> look in ~/.irods by default
  \t -a <admin user auth file> look in ~/.irods/.irodsA by default
  \t -u <client_user> (client user on behalf of which we are working)
  \t -m <md_manifest> (.JSON file - destination collection & metadata ops.)
  \t -v N (set verbosity level)
  ''' % (orig_argv[0],) , file = stderr )
  exit(1)

argv.insert(0,orig_argv[0])
options = {}
options.update( opt )

user = options.get('-u','')            # - client user
env_file = options.get( '-e', '')
if env_file == '-': env_file = expanduser('~/.irods/irods_environment.json' )
auth_file = options.get( '-a', '' )
if auth_file == '-': auth_file = expanduser('~/.irods/.irodsA')

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
                        'irods_authentication_file' :  auth_file,
                      }
session = None

k = options_for_session.keys()
for k_ in k: 
  if not options_for_session[k_]:
    del options_for_session[k_]

if not session:
    session = iRODSSession( **options_for_session )

if not user : user = 'rods'
else:
  if verbosity >= 1:
    print( "\tActing for user '{}'".format(session.username) , file = stderr )

if session:
  test_user = (user or admin_username)
  collName = '/tempZone/home/{}'.format(test_user)
  collObj = session.collections.get( collName  )
  dataObjs = collObj.data_objects
  print( "{!r}".format(dataObjs) , file = stderr )

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

