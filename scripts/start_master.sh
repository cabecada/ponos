#!/bin/bash

# set up zookeeper
sudo sh -c 'echo 1 > /etc/zookeeper/conf/myid'
myip=$(ifconfig | perl -nle'/dr:(\S+)/ && print $1' | grep -v 127.0.0.1)
sudo sh -c "echo server.1=$myip:2888:3888 >> /etc/zookeeper/conf/zoo.cfg"
sudo service zookeeper restart

# configure relevant mesos variables
sudo sh -c "echo zk://$myip:2181/mesos > /etc/mesos/zk"
sudo sh -c "echo 1 > /etc/mesos-master/quorum"

# stop slave
sudo service mesos-slave stop
sudo sh -c "echo manual > /etc/init/mesos-slave.override"

# start master
sudo service mesos-master restart


# python driver stuff shout out to
# https://github.com/ceteri/exelixi/blob/master/bin/local_install.sh
sudo apt-get -y install python-dev
sudo apt-get -y install python-pip
sudo apt-get -y install python-protobuf

sudo apt-get -y install git

# install mesos python bindings
EGG=mesos-0.20.0-py2.7-linux-x86_64.egg
wget http://downloads.mesosphere.io/master/ubuntu/14.04/$EGG
# sudo pip install $EGG
