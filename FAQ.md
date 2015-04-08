

# General #

## Is the store persisted on disk? ##

No its not persistent on disk (see also next question). Persistent storage is an often demanded feature, but it is not as easy to do as one might think. Lets explain why:

### Why persistent storage? ###

#### Persistent storage to overcome node failures and node resets: ####

Scalaris uses quorum algorithms, that assume a crash-stop failure model for
processes. A precondition of such systems is, that a majority of the replicas
of an item is always available.

If this precondition is violated, a majority of the nodes with replicas of an
item x is not available, the item cannot be changed. It is lost. Persistent
storage cannot help directly.

If a minority of the replicas fail, they will be repaired (new replicas are
created on other nodes).

If a single failed node does crash and recover, which is not foreseen in the
crash-stop model, but might happen if we have local persistent storage, we
have
three choices:
  1. drop the persistent storage and start as new node (crash-stop model)
  1. get some inconsistencies as another node already took over. For a short timeframe there might be more replicas in the system than allowed, which destroys the proper functioning of our majority based algorithms.
  1. friendly join the system and update the stored persistent state with the current state in the system (one way to implement that would be (1)).

So, persistent storage does not help in improving the availability or
robustness of the system.

#### Persistent storage to allow pausing the system (controlled shutdown and restart): ####

This would be possible to implement by a snapshot mechanism for
the system. To pause the system, one would cut user access, wait some time for
all ongoing transactions to finish, and then trigger a snapshot to local
disks. On startup, the nodes would continue from the last state after reading
the snapshot. Then user access can be restarted.

#### Persistent storage to have an always up-to-date snapshot of the system: ####
Persistence as in traditional replicated databases is not intended by our
system model and thereby not possible. The best alternative would be periodic
snapshots, that can be done also without interrupting the service.

## Can I store more data in Scalaris than ram+swapspace is available in the cluster? ##

Yes. We have several database backends, e.g. src/db\_ets.erl (ets) and src/db\_toke (tokyocabinet). The former uses the main memory for storing data, while the latter uses tokyocabinet for storing data on disk. With tokycoabinet, only your local disks should limit the total size of your database. Note however, that this still does not provide persistence.

For instructions on switching the database backend to tokyocabinet see [Tokyocabinet](Tokyocabinet.md).

# Installation #

## Minimum Requirements ##

  * Erlang >= R13B01 (see [Known Issues](FAQ#Known_Issues.md) below)

### Optional requirements ###

  * Java-API, CLI: Java >= 5, Ant
  * Python-API, CLI: Python >= 2.6 (including 3.x)
  * Ruby-API, CLI: Ruby >= 1.8
  * Storage on disk: Tokyocabinet, toke

## Prebuild packages ##

We build RPM and DEB packages for the newest tagged Scalaris version as well as periodic snapshots of svn trunk and provide them using the Open Build Service. The latest stable version is available at http://download.opensuse.org/repositories/home:/scalaris/. The latest svn snapshot is available at http://download.opensuse.org/repositories/home:/scalaris:/svn/.

Available packages are listed in the following table:

| **Distribution** | **Version** | **Download** |
|:-----------------|:------------|:-------------|
| Arch Linux |  | stable: http://download.opensuse.org/repositories/home:/scalaris/ArchLinux/<br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/ArchLinux/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/ArchLinux/</a> <br>
<tr><td> Fedora </td><td> 20 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/Fedora_20/'>http://download.opensuse.org/repositories/home:/scalaris/Fedora_20/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/Fedora_20/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/Fedora_20/</a> </td></tr>
<tr><td> Fedora </td><td> 21 </td><td> unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/Fedora_21/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/Fedora_21/</a> </td></tr>
<tr><td> openSUSE </td><td> 11.4 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/openSUSE_11.4/'>http://download.opensuse.org/repositories/home:/scalaris/openSUSE_11.4/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris/openSUSE_11.4/scalaris.ymp'>1-Click Install</a>)<br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_11.4/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_11.4/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_11.4/scalaris.ymp'>1-Click Install</a>) </td></tr>
<tr><td> openSUSE </td><td> 13.1 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/openSUSE_13.1/'>http://download.opensuse.org/repositories/home:/scalaris/openSUSE_13.1/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris/openSUSE_13.1/scalaris.ymp'>1-Click Install</a>)<br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_13.1/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_13.1/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_13.1/scalaris.ymp'>1-Click Install</a>) </td></tr>
<tr><td> openSUSE </td><td> 13.2 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/openSUSE_13.2/'>http://download.opensuse.org/repositories/home:/scalaris/openSUSE_13.2/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris/openSUSE_13.2/scalaris.ymp'>1-Click Install</a>)<br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_13.2/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_13.2/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_13.2/scalaris.ymp'>1-Click Install</a>) </td></tr>
<tr><td> openSUSE </td><td> Tumbleweed </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/openSUSE_Tumbleweed/'>http://download.opensuse.org/repositories/home:/scalaris/openSUSE_Tumbleweed/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris/openSUSE_Tumbleweed/scalaris.ymp'>1-Click Install</a>)<br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_Tumbleweed/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_Tumbleweed/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_Tumbleweed/scalaris.ymp'>1-Click Install</a>) </td></tr>
<tr><td> openSUSE </td><td> Factory </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/openSUSE_Factory/'>http://download.opensuse.org/repositories/home:/scalaris/openSUSE_Factory/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris/openSUSE_Factory/scalaris.ymp'>1-Click Install</a>)<br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_Factory/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_Factory/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/openSUSE_Factory/scalaris.ymp'>1-Click Install</a>) </td></tr>
<tr><td> SLE </td><td> 10 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/SLE_10/'>http://download.opensuse.org/repositories/home:/scalaris/SLE_10/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris/SLE_10/scalaris.ymp'>1-Click Install</a>)<br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_10/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_10/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_10/scalaris.ymp'>1-Click Install</a>) </td></tr>
<tr><td> SLE </td><td> 11 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/SLE_11/'>http://download.opensuse.org/repositories/home:/scalaris/SLE_11/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris/SLE_11/scalaris.ymp'>1-Click Install</a>)<br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_11/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_11/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_11/scalaris.ymp'>1-Click Install</a>) </td></tr>
<tr><td> SLE </td><td> 11 SP1 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/SLE_11_SP1/'>http://download.opensuse.org/repositories/home:/scalaris/SLE_11_SP1/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris/SLE_11_SP1/scalaris.ymp'>1-Click Install</a>)<br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_11_SP1/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_11_SP1/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_11_SP1/scalaris.ymp'>1-Click Install</a>) </td></tr>
<tr><td> SLE </td><td> 11 SP2 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/SLE_11_SP2/'>http://download.opensuse.org/repositories/home:/scalaris/SLE_11_SP2/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris/SLE_11_SP2/scalaris.ymp'>1-Click Install</a>)<br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_11_SP2/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_11_SP2/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_11_SP2/scalaris.ymp'>1-Click Install</a>) </td></tr>
<tr><td> SLE </td><td> 11 SP3 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/SLE_11_SP3/'>http://download.opensuse.org/repositories/home:/scalaris/SLE_11_SP3/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris/SLE_11_SP3/scalaris.ymp'>1-Click Install</a>)<br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_11_SP3/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_11_SP3/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_11_SP3/scalaris.ymp'>1-Click Install</a>) </td></tr>
<tr><td> SLE </td><td> 12 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/SLE_12/'>http://download.opensuse.org/repositories/home:/scalaris/SLE_12/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris/SLE_12/scalaris.ymp'>1-Click Install</a>)<br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_12/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_12/</a> (<a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/SLE_12/scalaris.ymp'>1-Click Install</a>) </td></tr>
<tr><td> CentOS </td><td> 5.5 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/CentOS_5/'>http://download.opensuse.org/repositories/home:/scalaris/CentOS_5/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/CentOS_5/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/CentOS_5/</a> </td></tr>
<tr><td> CentOS </td><td> 6.2 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/CentOS_6/'>http://download.opensuse.org/repositories/home:/scalaris/CentOS_6/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/CentOS_6/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/CentOS_6/</a> </td></tr>
<tr><td> CentOS </td><td> 7 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/CentOS_7/'>http://download.opensuse.org/repositories/home:/scalaris/CentOS_7/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/CentOS_7/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/CentOS_7/</a> </td></tr>
<tr><td> RHEL </td><td> 5.5 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/RHEL_5/'>http://download.opensuse.org/repositories/home:/scalaris/RHEL_5/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/RHEL_5/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/RHEL_5/</a> </td></tr>
<tr><td> RHEL </td><td> 6 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/RHEL_6/'>http://download.opensuse.org/repositories/home:/scalaris/RHEL_6/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/RHEL_6/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/RHEL_6/</a> </td></tr>
<tr><td> RHEL </td><td> 7 </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/RHEL_7/'>http://download.opensuse.org/repositories/home:/scalaris/RHEL_7/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/RHEL_7/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/RHEL_7/</a> </td></tr>
<tr><td> Debian </td><td> 6.0 (Squeeze) </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/Debian_6.0/'>http://download.opensuse.org/repositories/home:/scalaris/Debian_6.0/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/Debian_6.0/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/Debian_6.0/</a> </td></tr>
<tr><td> Debian </td><td> 7.0 (Wheezy) </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/Debian_7.0/'>http://download.opensuse.org/repositories/home:/scalaris/Debian_7.0/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/Debian_7.0/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/Debian_7.0/</a> </td></tr>
<tr><td> Ubuntu </td><td> 10.04 (Lucid Lynx) </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/xUbuntu_10.04/'>http://download.opensuse.org/repositories/home:/scalaris/xUbuntu_10.04/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/xUbuntu_10.04/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/xUbuntu_10.04/</a> </td></tr>
<tr><td> Ubuntu </td><td> 12.04 (Precise Pangolin) </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/xUbuntu_12.04/'>http://download.opensuse.org/repositories/home:/scalaris/xUbuntu_12.04/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/xUbuntu_12.04/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/xUbuntu_12.04/</a> </td></tr>
<tr><td> Ubuntu </td><td> 14.04 (Trusty Tahr) </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/xUbuntu_14.04/'>http://download.opensuse.org/repositories/home:/scalaris/xUbuntu_14.04/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/xUbuntu_14.04/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/xUbuntu_14.04/</a> </td></tr>
<tr><td> Ubuntu </td><td> 14.10 (Utopic Unicorn) </td><td> stable: <a href='http://download.opensuse.org/repositories/home:/scalaris/xUbuntu_14.10/'>http://download.opensuse.org/repositories/home:/scalaris/xUbuntu_14.10/</a><br>unstable: <a href='http://download.opensuse.org/repositories/home:/scalaris:/svn/xUbuntu_14.10/'>http://download.opensuse.org/repositories/home:/scalaris:/svn/xUbuntu_14.10/</a> </td></tr></tbody></table>

For those distributions which provide a recent-enough Erlang version, we build the packages using their Erlang package and recommend using the same version that came with the distribution. In this case we do not provide Erlang packages in our repository.<br>
<br>
Exceptions are made for RHEL-based distributions, SLE, openSUSE 11.4 and Arch Linux:<br>
<ul><li>For SLE and openSUSE 11.4, we provide Erlang R14B04.<br>
</li><li>For RHEL-based distributions (CentOS 5 & 6, RHEL 5 & 6) we include the Erlang package from the EPEL repository of RHEL 6.<br>
</li><li>For Arch Linux we include the Erlang package from the <a href='https://www.archlinux.org/packages/community/x86_64/erlang/'>community repository</a>.</li></ul>

<h3>How to add our repository to openSUSE-based distributions (openSUSE, SLE)?</h3>

For e.g. openSUSE 13.2 execute the following command as root:<br>
<pre><code>zypper addrepo --refresh http://download.opensuse.org/repositories/home:/scalaris/openSUSE_13.2/ scalaris<br>
</code></pre>
You can then either install Scalaris through YaST or use the command-line tool zypper: <code>zypper install scalaris</code>.<br>
<br>
<h3>How to add our repository to Fedora-based distributions (Fedora, RHEL, CentOS)?</h3>

To add our repository to the list of repositories, yum uses, download its description file and (as root) place it in <code>/etc/yum.repos.d/</code>, e.g. for Fedora 20:<br>
<pre><code>sudo curl "http://download.opensuse.org/repositories/home:/scalaris/Fedora_20/home:scalaris.repo" -o /etc/yum.repos.d/home\:scalaris.repo<br>
</code></pre>
. Afterwards you can install Scalaris using the usual <code>yum install scalaris</code> command (similarly for the other packages).<br>
<br>
<h4>Ruby on CentOS and RHEL</h4>

CentOS and RHEL do not come with the needed packages for our ruby bindings. If you have not already done so, add the appropriate EPEL repositories which provide them - follow <a href='https://fedoraproject.org/wiki/EPEL'>https://fedoraproject.org/wiki/EPEL</a> for further instructions.<br>
<br>
Note that the rubygem-json package provided by rpmforge for version 5 of CentOS and RHEL has a critical bug which makes it unusable - the EPEL packages have a fix (ref. <a href='https://bugzilla.redhat.com/show_bug.cgi?id=634380'>https://bugzilla.redhat.com/show_bug.cgi?id=634380</a>)<br>
<br>
<h3>How to add our repository to Mandriva-based distributions (Mandriva)?</h3>

This is currently not possible because the Open Build Service does not export a repository structure compatible with the Mandriva package management. You will need to manually download and install the RPMs from the repositories listed above.<br>
<br>
<h3>How to add our repository to Debian-based distributions (Debian, Ubuntu)?</h3>

Please note that the Open Build Service does not create a source repository for .deb-based distributions. To add one of our repositories to your System, add a line like the following to your sources.list (mostly in <code>/etc/apt/</code>). <i>Adapt the URL to the distribution you use (see above).</i>

<pre><code>deb http://download.opensuse.org/repositories/home:/scalaris/xUbuntu_14.04/ ./<br>
</code></pre>

Then download our package signing key and add it to your system (as root):<br>
<br>
<pre><code>wget -q http://download.opensuse.org/repositories/home:/scalaris/openSUSE_Factory/repodata/repomd.xml.key -O - | sudo apt-key add -<br>
</code></pre>

Afterwards, installing our Scalaris packages should be possible as usually, e.g. by calling<br>
<pre><code>apt-get update<br>
apt-get install scalaris<br>
</code></pre>

Refer to <a href='http://en.opensuse.org/openSUSE:Build_Service_Debian_builds#Configuring_sources.list'>http://en.opensuse.org/openSUSE:Build_Service_Debian_builds#Configuring_sources.list</a> if you are having trouble adding .deb-based repositories.<br>
<br>
<h3>How to add our repository to Arch Linux?</h3>

We offer official Scalaris packages in our OBS repositories but an externally-maintained package also exists in AUR. In order to use our repository, you can follow the following steps.<br>
<br>
First the public key needs to be added to the pacman keyring:<br>
<pre><code>wget -q http://download.opensuse.org/repositories/home:/scalaris/openSUSE_Factory/repodata/repomd.xml.key -O - | pacman-key -a -<br>
</code></pre>

Now the repository url can be added to <code>/etc/pacman.conf</code>
<pre><code>echo -e "[home_scalaris_ArchLinux]\nSigLevel = Optional TrustAll\nServer = http://download.opensuse.org/repositories/home:/scalaris/ArchLinux/\$arch/" &gt;&gt; /etc/pacman.conf<br>
</code></pre>

Now install scalaris and scalaris-bindings:<br>
<pre><code>pacman -Syy scalaris scalaris-bindings<br>
</code></pre>

<h2>How to build an rpm?</h2>
On openSUSE, for example, do the following:<br>
<pre><code>export SCALARIS_SVN=http://scalaris.googlecode.com/svn/trunk<br>
for package in main bindings ; do<br>
  mkdir -p ${package}<br>
  cd ${package}<br>
  svn export ${SCALARIS_SVN}/contrib/packages/${package}/checkout.sh<br>
  ./checkout.sh<br>
  cp * /usr/src/packages/SOURCES/<br>
  rpmbuild -ba scalaris*.spec<br>
  cd ..<br>
done<br>
</code></pre>

If any additional packages are required in order to build an RPM, rpmbuild will print an error.<br>
Your source and binary RPMs will be generated in <code>/usr/src/packages/SRPMS</code> and <code>RPMS</code>.<br>
<br>
<h2>Does Scalaris run on Windows?</h2>

No. Well, maybe.<br>
<br>
<ol><li>install Erlang (<a href='http://www.erlang.org/download.html'>http://www.erlang.org/download.html</a>)<br>
</li><li>install OpenSSL (for crypto module) (<a href='http://www.slproweb.com/products/Win32OpenSSL.html'>http://www.slproweb.com/products/Win32OpenSSL.html</a>)<br>
</li><li>checkout Scalaris code from SVN<br>
</li><li>adapt the path to your Erlang installation in build.bat<br>
</li><li>start a <code>cmd.exe</code>
</li><li>go to the Scalaris directory<br>
</li><li>run <code>build.bat</code> in the cmd window<br>
</li><li>check that there were no errors during the compilation; warnings are fine<br>
</li><li>go to the <code>bin</code> sub-directory<br>
</li><li>adapt the path to your Erlang installation in <code>firstnode.bat</code>, and <code>joining_node.bat</code>
</li><li>run <code>firstnode.bat</code> in the cmd window</li></ol>

If you have Erlang < R13B04, you will need to adapt the <code>Emakefile</code> that <code>build.bat</code> generates if it doesn't exist.<br>
Replace every occurrence of<br>
<pre><code>, {d, tid_not_builtin} <br>
</code></pre>
with these lines (note the first ","!) and try to compile again, using build.bat. It should work now.<br>
<pre><code>, {d, type_forward_declarations_are_not_allowed}, {d, forward_or_recursive_types_are_not_allowed}<br>
</code></pre>

Note: we do not support Scalaris on Windows at the moment.<br>
<br>
<br>
<h1>Configuration</h1>

<h2>Which ports are used by Scalaris?</h2>

If you have a firewall on your servers, you have to open the following ports. If you run Scalaris on Amazon EC2, you have to open these ports in the management console for your security group.<br>
<br>
<table><thead><th> Port  </th><th> Purpose                             </th><th> How to change </th></thead><tbody>
<tr><td> 14195 </td><td> Used for all internal communication </td><td> see <a href='FAQ#How_can_I_change_the_port_used_for_internal_communication?.md'>FAQ#How_can_I_change_the_port_used_for_internal_communication?</a></td></tr>
<tr><td> 8000  </td><td> Web-server and JSON-RPC             </td><td> see <a href='FAQ#How_can_I_change_the_web_server_port/yaws_port?.md'>FAQ#How_can_I_change_the_web_server_port/yaws_port?</a> </td></tr>
<tr><td> 4369  </td><td> necessary, if you want to access Scalaris via the Java-API from a remote node (epmd) </td><td> - </td></tr>
<tr><td> random port </td><td> necessary, if you want to access Scalaris via the Java-API from a remote node </td><td> <a href='FAQ#How_can_I_change_the_ports_used_for_the_Java-API?.md'>FAQ#How_can_I_change_the_ports_used_for_the_Java-API?</a></td></tr></tbody></table>

<h2>How can I change the port used for internal communication?</h2>

When you start Scalaris using <code>scalarisctl</code>, the parameter <code>-p</code> can change the port.<br>
<br>
<code>scalarisctl -p THEPORT</code>

<h2>How can I change the ports used for the Java-API?</h2>

The epmd is a Erlang daemon which always listens on port 4369. It provides means to lookup the port of Erlang processes by their name. Every time Scalaris starts, it will listen on a different port. But you can limit the range from which Scalaris selects this port. In the following example, we will limit the range to just 14194.<br>
<br>
<code>scalarisctl -e "-kernel inet_dist_listen_min 14194 inet_dist_listen_max 14194"</code>

<code>scalarisctl</code> from svn trunk contains <code>the --dist-erl-port</code> to ease this procedure:<br>
<br>
<code>scalarisctl --dist-erl-port 14194</code>

<h2>How can I change the web server port/yaws port?</h2>

For changing the yaws port of the first node or a normal node change the <code>YAWSPORT</code> variable in its start script, e.g. <code>bin/firstnode.sh</code> or <code>bin/joining_node.sh</code>.<br>
<br>
Using <code>scalarisctl</code>, the parameter <code>-y</code> influences the port used by yaws:<br>
<br>
<code>scalarisctl -y 8000</code>

<h1>Usage</h1>

<h2>What is the difference between a management-server and an ordinary node?</h2>

Scalaris nodes can take two roles:<br>
<ul><li>a management-server and<br>
</li><li>a Scalaris node.</li></ul>

As a management-server a Scalaris node maintains a list of all Scalaris nodes in the system and provides a web-interface for debugging - including different plots of the ring. It has no administrative tasks. A ring could be created without one.<br>
<br>
Note that <code>./bin/firstnode.sh</code> starts an Erlang VM with both, a management-server and a Scalaris node which is marked as the <i>first</i> node.<br>
<br>
<h2>Why is one Scalaris node marked as <i>first</i>?</h2>

When starting a Scalaris ring we have to mark exactly one node as the first node. This node will not try contacting any other node and simply wait for connections. It thus creates a new ring.<br>
<br>
We are currently using a command line parameter <code>-scalaris first true</code> to mark the first Scalaris node inside an Erlang VM as being the first (see <code>bin/scalarisctl</code> and <code>dht_node:init/1</code>).<br>
<br>
For a fault-tolerant setup, Scalaris is able to select the first node by itself from the list of known_hosts. Scalaris will then start, if a majority of the known hosts becomes available (See -q in <code>bin/scalarisctl</code>). In this setup no node has to be marked first explicitly.<br>
<br>
<h2>How do I start a ring?</h2>

<pre><code>./bin/firstnode.sh`<br>
</code></pre>

starts both a boot server and a Scalaris node - a ring of size 1.<br>
Further clients can be started using<br>
<pre><code>./bin/joining_node.sh<br>
./bin/joining_node.sh 2<br>
./bin/joining_node.sh 3<br>
</code></pre>

Alternatively, <code>scalarisctl -m start</code> starts a management-server and <code>scalarisctl -s start</code> starts a node similar to <code>joining_node.sh</code>. A Scalaris node can be marked as first using the parameter <code>-f</code>. To start Scalaris in the background, use <code>-d</code>. Refer to the help <code>scalarisctl -h</code> for further options.<br>
<br>
<h2>How do I delete a key?</h2>

The principle issues with deleting keys from Scalaris are described in<br>
<a href='http://groups.google.com/group/scalaris/browse_thread/thread/ff1d9237e218799'>this</a> thread of the mailing list.<br>
<br>
In short: deleting a key may violate one of Scalaris' fundamental assumptions - version numbers of keys never decrease. Everything is fine, as long as all replicas are deleted. If some remain, because a node was not available during the delete, you may get into trouble.<br>
<br>
Keys can be deleted outside transactions from Erlang and Java.<br>
<br>
In Erlang you call api_rdht:delete(Key) which returns<br>
{ok, pos_integer(), list()} | {fail, timeout} |<br>
{fail, timeout, pos_integer(), list()} | {fail, node_not_found}).<br>
The pos_integer() indicates how many replicas were successfully deleted, and the list contains further details.<br>
<br>
In Java you can call delete(key) or delete(key, timeout) from the Scalaris object and get further details on the result by calling getLastDeleteResult().<br>
<br>
<br>
<h2>What does the "discarded messages" error message mean?</h2>

If you stop a Scalaris node and immediately restart it, you can get messages like the following:<br>
<pre><code>[error] Discarding message {ping,{{127,0,0,1},14195,&lt;9148.128.0&gt;}} from &lt;0.108.0&gt; to &lt;0.102.0&gt; in an old incarnation (3) of this node (1)<br>
</code></pre>
It is not so much an error but a message created by the Erlang runtime that this particular message was sent to the last "incarnation" of this Scalaris node. Here it is the failure detector sending a ping message. After a couple of seconds, the failure detector will notice the change and the messages will stop to appear.<br>
<br>
<h2>Java-API</h2>

<h3>The Java-client cannot connect to Scalaris. What is wrong?</h3>

If you do not use our client script (java-api/scalaris) your hostname configuration probably needs some adjustments. Erlang and Java use different mechanisms to determine the full qualified domain name (FQDN), but both have to come to the same result in order to be able to connect.<br>
<br>
You may check your installation using:<br>
<br>
<pre><code>scalarisctl checkinstallation<br>
</code></pre>

which will guide you further.<br>
<br>
<ul><li>For getting the node name Erlang uses, call:<br>
<pre><code>    erl -noinput -name boot -eval 'io:format("~s~n", [node()]), halt().'<br>
</code></pre>
<blockquote>If this gives you a strange erlang error, replace "boot" in the line above with some arbitrary name.<br>
</blockquote></li><li>For getting the hostname the Java-API uses, call:<br>
<ul><li>installed with an RPM:<br>
<pre><code>      scalaris --noerl -lh<br>
</code></pre>
</li><li>inside the source directory:<br>
<pre><code>      ./java-api/scalaris --noconfig --noerl -lh<br>
</code></pre></li></ul></li></ul>

The domain name from Erlang should be the same as the name printed by the Java client. If not, you may need to change your <code>/etc/hosts</code> or <code>/etc/hostname</code> for it to work.<br>
<br>
<h2>Python-API</h2>

<h3>Reading a list of (small) integers results in a unicode object instead of a list.</h3>

Unfortunately, Erlang can not distinguish between list of (small) integers and strings (which are lists of small integers, internally). Therefore, we can not guess the resulting type neither in the Java API, nor in the Python API.<br>
The Java API contains several ???Value() methods for the values read back from<br>
Scalaris.<br>
Since python is dynamically typed, most of these are not needed except for the<br>
conversion from (falsely interpreted) strings to lists of integers. Use our <code>str_to_list()</code> utility method form the <code>Scalaris</code> module for this task.<br>
<br>
Note: You can safely use <code>Scalaris.str_to_list()</code> for any integer list (even those with large integers which are already returned as valid lists). It will return the original value if it is not a string or unicode object.<br>
<br>
<h1>Known Issues</h1>

<h2>Publishing topics with Erlang R13B01 may fail</h2>

The httpc_handler module of the R13B01 distribution sporadically crashes when a message is published to a web server. This can be seen using our integrated java tests. There have been some changes to that module in R13B02 which seem to have fixed this bug. We haven't observed this behavior in Erlang R13B02 - R14A.<br>
<br>
<h2>Configure claims that the Java client script may not work</h2>

Our client script wrapper in <code>java-api/scalaris</code> uses <code>java-functions</code> and <code>build-classpath</code> to cope with distribution-specific paths. If either of them is not present, the client script <i>may</i> not work. The following explains a bit more what they do and how they are used:<br>
<br>
<ul><li><code>java-functions</code> (if present) will be used to setup the java environment before starting the client<br>
</li><li><code>build-classpath</code> is a script that eases classpath creation especially when a script is being used on different platforms. If <code>build-classpath</code> is not present, our wrapper script will try to locate dependent jar files in <code>$SCALARIS_JAVA_HOME/lib</code> - these jars include <code>OtpErlang-1.5.4.jar</code> and <code>jakarta-commons-cli-1.2.jar</code> which can be copied from <code>./java-api/lib/</code>. If unset, SCALARIS_JAVA_HOME will be set to the client wrapper script's location which enables the script to be called from a checkout of the scalaris sources.