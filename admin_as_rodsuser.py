
from __future__ import print_function

import os, sys
import irods
from irods.meta import iRODSMeta
from irods.session import iRODSSession
from irods.data_object import iRODSReplica
from os.path import expanduser, curdir, sep ,abspath, join
from sys import argv as orig_argv, stderr, stdout, exit
from getopt import GetoptError, getopt
import json


# ////////////////
#
# Sample usage:
#
# admin_as_rodsuser.py [ opts ... ] [ -u clientUserName ]
#   miscellaneous opts:
#             -p /file/to/be/registered 
#             -m mdmanifest_file.json 
#             -u clientUserName
#
# ////////////////

def replicate_and_list_good_replicas (session , logical_path ,  resc_name='',  # -> inputs
                                      list_for_tally = () ):                   # -> output

  data_obj = session.data_objects.get( logical_path )
  data_obj .replicate (resc_name )

  data_obj = session.data_objects.get( logical_path )

  replicas = [ repl for repl in data_obj.replicas 
               if repl.status == '1' ]
  
  if type( list_for_tally ) in (list, tuple):

    existing_repl_ids = [ x.number for x in list_for_tally if type(x) is iRODSReplica ]

    new_list = list (list_for_tally)

    for x in replicas:
      if x.number not in existing_repl_ids:
        new_list.append (x)

    if type(list_for_tally) is list: list_for_tally[:] = new_list

  return new_list

#--------------------------------------------------------

try:
  opt, argv = getopt( orig_argv[1:], 'p:r:f:u:m:v:e:t:' )
except GetoptError:
  print ( '''Usage: %s [options]
  where options are:
   -h help  (print this usage statement)
   -p physical path (file to register into iRODS)
   -r immediate_resc_name
        (name of storage resource for immediate registration of product)
   -f final_resc_name
        (name of storage resource for long-term storage of product)
   -e <admin user irods_environment.json> look in ~/.irods by default
   -a <admin user auth file> look in ~/.irods/.irodsA by default
   -u <client_user> (client user on behalf of which we are working)
   -m <md_manifest> (.JSON file - destination collection & metadata ops.)
   -v N (set verbosity level)
   -t <g|m|l|t> m - add metadata ; g - register products ; l - replicate ; t - trim
  ''' % (orig_argv[0],) , file = stderr )
  exit(1)

argv.insert(0,orig_argv[0])
options = {}
options.update( opt )

user = options.get('-u','')            # - client user

env_file = options.get( '-e', '')

if not(env_file) or env_file == '-':
  env_file = expanduser('~/.irods/irods_environment.json' )

auth_file = options.get( '-a' )

if auth_file is not None:
  if auth_file in ('', '-'):
    auth_file = expanduser('~/.irods/.irodsA')
else:
  auth_file = ''

phyP = options.get('-p' ,'')
rescN = options.get('-r','')
long_term_rescN = options.get('-f','')

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

k = options_for_session.keys()
for k_ in k: 
  if not options_for_session[k_]:
    del options_for_session[k_]

with iRODSSession( **options_for_session ) as session:

  if not user : user = 'rods'
  else:
    if verbosity >= 1:
      print( "\tActing for user '{}'".format(session.username) , file = stderr )

  md_manifest_file = options['-m']

  if not(phyP) or not(os.path.isfile(phyP)):
    print ("invalid physical file path '{}'\n\t" \
	   "as source for iRODS register()".format(phyP), file=sys.stderr)
    raise SystemExit(1)

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
    try:
      parentColl = session.collections.create(parentCollName)
    except: pass

  assert parentColl, "Could not find or create collection in which to register " +\
		     "{}".format(phyP)

  if not(rescN):
    del register_opts['rescName'] 
  else:
    register_opts['rescName'] = rescN

  data_object_basename = os.path.basename (phyP)
  logP = parentColl.path + "/" + data_object_basename
    
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

    task_options = options.get('-t','gmtl')

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    if 'g' in task_options:
      print ( '** Registering **' , file = stderr )
      session.data_objects.register ( phyP , logP , **register_opts )

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    if 'm' in task_options:
      print ( '** Setting metadata **', file = stderr)
      data_obj = session.data_objects.get( logP )
      physical_path_for_data_obj = data_obj.path
      for md_op in mdManifest.get("operation", [] ):
	action = md_op["action"] 
	if action == 'ADD':
	  if md_op["irodsPath"] == os.path.basename ( data_obj.path ):
	    md_record = iRODSMeta ( md_op["attribute"],md_op["value"],md_op["unit"] )
	    data_obj.metadata.add ( md_record )
	else:
	  print (" unimplemented feature '{}' requested in mdmanifest file '{}'" )
	
    # -- replicate product to long term storage

    if 'l' in task_options:

      repls_list = replicate_and_list_good_replicas (session , logP ,  long_term_rescN )

    # -- remove any redundant product replicas

    if 't' in task_options:

      redundant_repls = [ r for r in repls_list if r.resource_name != long_term_rescN ]

      if redundant_repls and len(redundant_repls) < len(repls_list):
	for r in redundant_repls:
	   data_obj.unlink ( replNum = r.number)

  # end if # answer.upper().strip() == 'Y':

#end #  with iRODSSession(...) as session
