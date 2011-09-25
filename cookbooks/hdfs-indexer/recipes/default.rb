#
# Cookbook Name:: hdfs-indexer
# Recipe:: default
#
# Copyright 2011, Robin Wenglewski
#
# All rights reserved - Do Not Redistribute
#

raise "this recipe is only tested on ubuntu" unless platform?("ubuntu")

include_recipe "hadoop-alt"

execute "aptitude update"
package "git-core"

# download my gist