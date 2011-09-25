#
# Cookbook Name:: hdfs-indexer
# Recipe:: default
#
# Copyright 2011, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#

execute "apt-get update" do
  command "apt-get update"
  action :nothing
  only_if { platform?("ubuntu", "debian") }
end

