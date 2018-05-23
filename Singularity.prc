Bootstrap: docker
From: holbertonschool/base-ubuntu-1404

%post
   apt-get update
   apt-get -y install apt-transport-https
   apt-get -y install python-pip
   pip install python-irodsclient
