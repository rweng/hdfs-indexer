task :setup do
  deps = %w-ruby gem virtualbox-
  unless deps.inject(true){|prev,dep| prev and %x(which #{dep}) ? true : false }
    raise "you need to install the following dependencies: #{deps.join( )}"
  end
  
  puts "installling git submodules"
  %x(git submodule init && git submodule update)
  
  puts "installing vagrant gem"
  %x(gem install vagrant)
  
  puts "adding lucid-32 box to vagrant. this could take some time"
  %x(vagrant box add ubuntu-lucid-32 http://files.vagrantup.com/lucid32.box)
  
  puts "you can now run 'vagrant up' to create your virtual machine"
end