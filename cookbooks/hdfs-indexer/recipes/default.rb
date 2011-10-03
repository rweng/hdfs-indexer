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
include_recipe "tpch"

package "git-core"