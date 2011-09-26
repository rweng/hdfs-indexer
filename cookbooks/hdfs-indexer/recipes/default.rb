#
# Cookbook Name:: hdfs-indexer
# Recipe:: default
#
# Copyright 2011, Robin Wenglewski
#
# All rights reserved - Do Not Redistribute
#

include_recipe "hadoop::cloudera"
include_recipe "apt"
include_recipe "gradle"

package "git-core"

execute "cd /vagrant; gradle jar"