maintainer       "Robin Wenglewski"
maintainer_email "robin"
license          "All rights reserved"
description      "Installs/Configures hdfs-indexer"
long_description IO.read(File.join(File.dirname(__FILE__), 'README.rdoc'))
version          "0.0.1"

depends %w-apt java hadoop tpch-

recipe "hdfs-indexer", "installs all deps for running the hdfs-indexer"

%w{ ubuntu }.each do |os|
  supports os
end