maintainer       "Zuse Institute Berlin"
maintainer_email "kruber@zib.de"
license          "Apache 2.0"
description      "Installs/Configures Scalaris for the 4CaaSt platform"
#long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))
version          "0.0.1"

depends "apt", "= 1.1.2"

%w{ debian ubuntu centos redhat fedora suse }.each do |os|
  supports os
end

recipe "scalaris_PIC::Deploy_PIC", "Installs and configures Scalaris"
recipe "scalaris_PIC::Start_PIC", "Starts Scalaris"
recipe "scalaris_PIC::Stop_PIC", "Stops Scalaris"
recipe "scalaris_PIC::Undeploy_PIC", "Undeploys Scalaris"
