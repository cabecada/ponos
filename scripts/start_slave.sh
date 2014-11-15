#!/bin/bash

# takes a single argument, the ip of the master

# stop zookeeper
sudo service zookeeper stop
sudo sh -c "echo manual > /etc/init/zookeeper.override"

# say where the master is
sudo sh -c "echo zk://$1:2181/mesos > /etc/mesos/zk"

# stop master
sudo service mesos-master stop
sudo sh -c "echo manual > /etc/init/mesos-master.override"

# start slave
sudo service mesos-slave restart
