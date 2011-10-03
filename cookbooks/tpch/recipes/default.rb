#
# Cookbook Name:: tpch
# Recipe:: default
#
# Copyright 2011, Robin Wenglewski
#
# All rights reserved - Do Not Redistribute
#

remote_file "/tmp/tpch_2_14_0.tgz" do
  source "http://www.tpc.org/tpch/spec/tpch_2_14_0.tgz"
  not_if "test -f /tmp/tpch_2_14_0.tgz"
end

script "extract and move tpch archive" do
  interpreter "bash"
  user "root"
  cwd "/tmp"
  code "tar xzf tpch_2_14_0.tgz; mv dbgen /usr/local/"
end

template '/usr/local/dbgen/makefile.suite' do
  source 'makefile.suite'
end

script "make dbgen" do
  cwd '/usr/local/dbgen'
  interpreter "bash"
  user 'root'
  code 'make -f makefile.suite; chmod -R 755 .'
end

# dbgen has to be executed from /usr/local/dbgen directory since we cant figure out how to link it statically
link '/usr/local/bin/dbgen' do
  to '/usr/local/dbgen/dbgen'
end
