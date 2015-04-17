---
title: Scalaris &mdash; FAQ
root: .
layout: default
---

# Frequently Asked Questions

* auto-gen TOC:
{:toc}

## General

### Is the store persisted on disk?

No its not persistent on disk (see also next question). Persistent storage is an often demanded feature, but it is not as easy to do as one might think. Lets explain why:

#### Why persistent storage?

##### Persistent storage to overcome node failures and node resets:

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

0. drop the persistent storage and start as new node (crash-stop model)
0. get some inconsistencies as another node already took over. For a short timeframe there might be more replicas in the system than allowed, which destroys the proper functioning of our majority based algorithms.
0. friendly join the system and update the stored persistent state with the current state in the system (one way to implement that would be (1)).

So, persistent storage does not help in improving the availability or
robustness of the system.

##### Persistent storage to allow pausing the system (controlled shutdown and restart):

This would be possible to implement by a snapshot mechanism for
the system. To pause the system, one would cut user access, wait some time for
all ongoing transactions to finish, and then trigger a snapshot to local
disks. On startup, the nodes would continue from the last state after reading
the snapshot. Then user access can be restarted.

##### Persistent storage to have an always up-to-date snapshot of the system:
Persistence as in traditional replicated databases is not intended by our
system model and thereby not possible. The best alternative would be periodic
snapshots, that can be done also without interrupting the service.

### Can I store more data in Scalaris than ram+swapspace is available in the cluster?

Yes. We have several database backends, e.g. src/db_ets.erl (ets) and src/db_toke (tokyocabinet). The former uses the main memory for storing data, while the latter uses tokyocabinet for storing data on disk. With tokycoabinet, only your local disks should limit the total size of your database. Note however, that this still does not provide persistence.

For instructions on switching the database backend to tokyocabinet see [Tokyocabinet].

## Installation

### Minimum Requirements

  * Erlang >= R13B01 (see [Known Issues](#known-issues) below)

#### Optional requirements

  * Java-API, CLI: Java >= 5, Ant
  * Python-API, CLI: Python >= 2.6 (including 3.x)
  * Ruby-API, CLI: Ruby >= 1.8
  * Storage on disk: Tokyocabinet, toke

### Prebuild packages

We build RPM and DEB packages for the newest tagged Scalaris version
as well as periodic snapshots of git HEAD and provide them using the Open Build Service. The latest stable version is available at [http://download.opensuse.org/repositories/home:/scalaris/](http://download.opensuse.org/repositories/home:/scalaris/). The latest git snapshot is available at [http://download.opensuse.org/repositories/home:/scalaris:/svn/](http://download.opensuse.org/repositories/home:/scalaris:/svn/).

For those distributions which provide a recent-enough Erlang version, we build the packages using their Erlang package and recommend using the same version that came with the distribution. In this case we do not provide Erlang packages in our repository.

Exceptions are made for RHEL-based distributions, SLE, openSUSE 11.4
and Arch Linux:

  * For SLE and openSUSE 11.4, we provide Erlang R14B04.
  * For RHEL-based distributions (CentOS 5 & 6, RHEL 5 & 6) we include the Erlang package from the EPEL repository of RHEL 6.
  * For Arch Linux we include the Erlang package from the [https://www.archlinux.org/packages/community/x86_64/erlang/](https://www.archlinux.org/packages/community/x86_64/erlang/) community repository].

#### How to add our repository to openSUSE-based distributions (openSUSE, SLE)?

For e.g. openSUSE 13.2 execute the following command as root:
{% highlight sh %}
zypper addrepo --refresh http://download.opensuse.org/repositories/home:/scalaris/openSUSE_13.2/ scalaris
{% endhighlight %}
You can then either install Scalaris through YaST or use the command-line tool zypper: `zypper install scalaris`.

#### How to add our repository to Fedora-based distributions (Fedora, RHEL, CentOS)?

To add our repository to the list of repositories, yum uses, download its description file and (as root) place it in `/etc/yum.repos.d/`, e.g. for Fedora 20:
{% highlight sh %}
sudo curl "http://download.opensuse.org/repositories/home:/scalaris/Fedora_20/home:scalaris.repo" -o /etc/yum.repos.d/home\:scalaris.repo
{% endhighlight %}
Afterwards you can install Scalaris using the usual `yum install scalaris` command (similarly for the other packages).

##### Ruby on CentOS and RHEL

CentOS and RHEL do not come with the needed packages for our ruby bindings. If you have not already done so, add the appropriate EPEL repositories which provide them - follow [https://fedoraproject.org/wiki/EPEL](https://fedoraproject.org/wiki/EPEL) for further instructions.

Note that the rubygem-json package provided by rpmforge for version 5 of CentOS and RHEL has a critical bug which makes it unusable - the EPEL packages have a fix (ref. [https://bugzilla.redhat.com/show_bug.cgi?id=634380](https://bugzilla.redhat.com/show_bug.cgi?id=634380))

#### How to add our repository to Mandriva-based distributions (Mandriva)?

This is currently not possible because the Open Build Service does not export a repository structure compatible with the Mandriva package management. You will need to manually download and install the RPMs from the repositories listed above.

#### How to add our repository to Debian-based distributions (Debian, Ubuntu)?

Please note that the Open Build Service does not create a source repository for .deb-based distributions. To add one of our repositories to your System, add a line like the following to your sources.list (mostly in `/etc/apt/`). _Adapt the URL to the distribution you use (see above)._

{% highlight sh %}
deb http://download.opensuse.org/repositories/home:/scalaris/xUbuntu_14.04/ ./
{% endhighlight %}

Then download our package signing key and add it to your system (as root):

{% highlight sh %}
wget -q http://download.opensuse.org/repositories/home:/scalaris/openSUSE_Factory/repodata/repomd.xml.key -O - | sudo apt-key add -
{% endhighlight %}

Afterwards, installing our Scalaris packages should be possible as usually, e.g. by calling
{% highlight sh %}
apt-get update
apt-get install scalaris
{% endhighlight %}

Refer to [http://en.opensuse.org/openSUSE:Build_Service_Debian_builds#Configuring_sources.list](http://en.opensuse.org/openSUSE:Build_Service_Debian_builds#Configuring_sources.list) if you are having trouble adding .deb-based repositories.

#### How to add our repository to Arch Linux?

We offer official Scalaris packages in our OBS repositories but an externally-maintained package also exists in AUR. In order to use our repository, you can follow the following steps.

First the public key needs to be added to the pacman keyring:
{% highlight sh %}
wget -q http://download.opensuse.org/repositories/home:/scalaris/openSUSE_Factory/repodata/repomd.xml.key -O - | pacman-key -a -
{% endhighlight %}

Now the repository url can be added to `/etc/pacman.conf`
{% highlight sh %}
echo -e "[home_scalaris_ArchLinux]\nSigLevel = Optional TrustAll\nServer = http://download.opensuse.org/repositories/home:/scalaris/ArchLinux/\$arch/" >> /etc/pacman.conf
{% endhighlight %}

Now install scalaris and scalaris-bindings:
{% highlight sh %}
pacman -Syy scalaris scalaris-bindings
{% endhighlight %}

### How to build an rpm?
On openSUSE, for example, do the following:
{% highlight sh %}
export SCALARIS_SVN=http://scalaris.googlecode.com/svn/trunk
for package in main bindings ; do
  mkdir -p ${package}
  cd ${package}
  svn export ${SCALARIS_SVN}/contrib/packages/${package}/checkout.sh
  ./checkout.sh
  cp * /usr/src/packages/SOURCES/
  rpmbuild -ba scalaris*.spec
  cd ..
done
{% endhighlight %}

If any additional packages are required in order to build an RPM, rpmbuild will print an error.
Your source and binary RPMs will be generated in `/usr/src/packages/SRPMS` and `RPMS`.

### Does Scalaris run on Windows?

No. Well, maybe.

  0. install Erlang ([http://www.erlang.org/download.html](http://www.erlang.org/download.html))
  0. install OpenSSL (for crypto module) ([http://www.slproweb.com/products/Win32OpenSSL.html](http://www.slproweb.com/products/Win32OpenSSL.html))
  0. checkout Scalaris code from SVN
  0. adapt the path to your Erlang installation in build.bat
  0. start a `cmd.exe`
  0. go to the Scalaris directory
  0. run `build.bat` in the cmd window
  0. check that there were no errors during the compilation; warnings are fine
  0. go to the `bin` sub-directory
  0. adapt the path to your Erlang installation in `firstnode.bat`, and `joining_node.bat`
  0. run `firstnode.bat` in the cmd window

If you have Erlang < R13B04, you will need to adapt the `Emakefile` that `build.bat` generates if it doesn't exist.
Replace every occurrence of
{% highlight sh %}
, {d, tid_not_builtin}
{% endhighlight %}
with these lines (note the first ","!) and try to compile again, using build.bat. It should work now.
{% highlight sh %}
, {d, type_forward_declarations_are_not_allowed}, {d, forward_or_recursive_types_are_not_allowed}
{% endhighlight %}

Note: we do not support Scalaris on Windows at the moment.


## Configuration

### Which ports are used by Scalaris?

If you have a firewall on your servers, you have to open the following ports. If you run Scalaris on Amazon EC2, you have to open these ports in the management console for your security group.

|| **Port**  || **Purpose**                             || **How to change** ||
|| 14195 || Used for all internal communication || [see below](#how-can-i-change-the-port-used-for-internal-communication)||
|| 8000  || Web-server and JSON-RPC             || [see below](#how-can-i-change-the-web-server-portyaws-port) ||
|| 4369  || necessary, if you want to access Scalaris via the Java-API from a remote node (epmd) || - ||
|| random port || necessary, if you want to access Scalaris via the Java-API from a remote node || [see below](#how-can-i-change-the-ports-used-for-the-java-api) ||

### How can I change the port used for internal communication?

When you start Scalaris using `scalarisctl`, the parameter `-p` can change the port.

`scalarisctl -p THEPORT`

### How can I change the ports used for the Java-API?

The epmd is a Erlang daemon which always listens on port 4369. It provides means to lookup the port of Erlang processes by their name. Every time Scalaris starts, it will listen on a different port. But you can limit the range from which Scalaris selects this port. In the following example, we will limit the range to just 14194.

`scalarisctl -e "-kernel inet_dist_listen_min 14194 inet_dist_listen_max 14194"`

`scalarisctl` from svn trunk contains `the --dist-erl-port` to ease this procedure:

`scalarisctl --dist-erl-port 14194`

### How can I change the web server port/yaws port?

For changing the yaws port of the first node or a normal node change the `YAWSPORT` variable in its start script, e.g. `bin/firstnode.sh` or `bin/joining_node.sh`.

Using `scalarisctl`, the parameter `-y` influences the port used by yaws:

`scalarisctl -y 8000`

## Usage

### What is the difference between a management-server and an ordinary node?

Scalaris nodes can take two roles:

* a management-server and
* a Scalaris node.

As a management-server a Scalaris node maintains a list of all Scalaris nodes in the system and provides a web-interface for debugging - including different plots of the ring. It has no administrative tasks. A ring could be created without one.

Note that `./bin/firstnode.sh` starts an Erlang VM with both, a management-server and a Scalaris node which is marked as the _first_ node.

### Why is one Scalaris node marked as _first_?

When starting a Scalaris ring we have to mark exactly one node as the first node. This node will not try contacting any other node and simply wait for connections. It thus creates a new ring.

We are currently using a command line parameter `-scalaris first true` to mark the first Scalaris node inside an Erlang VM as being the first (see `bin/scalarisctl` and `dht_node:init/1`).

For a fault-tolerant setup, Scalaris is able to select the first node by itself from the list of known_hosts. Scalaris will then start, if a majority of the known hosts becomes available (See -q in `bin/scalarisctl`). In this setup no node has to be marked first explicitly.

### How do I start a ring?

{% highlight sh %}
./bin/firstnode.sh`
{% endhighlight %}

starts both a boot server and a Scalaris node - a ring of size 1.
Further clients can be started using
{% highlight sh %}
./bin/joining_node.sh
./bin/joining_node.sh 2
./bin/joining_node.sh 3
{% endhighlight %}

Alternatively, `scalarisctl -m start` starts a management-server and `scalarisctl -s start` starts a node similar to `joining_node.sh`. A Scalaris node can be marked as first using the parameter `-f`. To start Scalaris in the background, use `-d`. Refer to the help `scalarisctl -h` for further options.

### How do I delete a key?

The principle issues with deleting keys from Scalaris are described in
[http://groups.google.com/group/scalaris/browse_thread/thread/ff1d9237e218799 this] thread of the mailing list.

In short: deleting a key may violate one of Scalaris' fundamental assumptions - version numbers of keys never decrease. Everything is fine, as long as all replicas are deleted. If some remain, because a node was not available during the delete, you may get into trouble.

Keys can be deleted outside transactions from Erlang and Java.

In Erlang you call api_rdht:delete(Key) which returns
{ok, pos_integer(), list()} | {fail, timeout} |
{fail, timeout, pos_integer(), list()} | {fail, node_not_found}).
The pos_integer() indicates how many replicas were successfully deleted, and the list contains further details.

In Java you can call delete(key) or delete(key, timeout) from the Scalaris object and get further details on the result by calling getLastDeleteResult().


### What does the "discarded messages" error message mean?

If you stop a Scalaris node and immediately restart it, you can get messages like the following:
{% highlight sh %}
[error] Discarding message {ping,{ {127,0,0,1},14195,<9148.128.0>}} from <0.108.0> to <0.102.0> in an old incarnation (3) of this node (1)
{% endhighlight %}
It is not so much an error but a message created by the Erlang runtime that this particular message was sent to the last "incarnation" of this Scalaris node. Here it is the failure detector sending a ping message. After a couple of seconds, the failure detector will notice the change and the messages will stop to appear.

### Java-API

#### Maven repository

Alternatively to installing the Java-API from the sources or using the [packages](#prebuild-packages), we also provide a Maven repository at [http://scalaris.zib.de/maven](http://scalaris.zib.de/maven). To use this repository, add it to your `pom.xml`:

{% highlight xml %}
<dependencies>
  <dependency>
    <groupId>de.zib.scalaris</groupId>
    <artifactId>java-api</artifactId>
      <version>[0.7.2,)</version>
  </dependency>
</dependencies>

<repositories>
  <repository>
    <id>scalaris-repo</id>
    <url>http://scalaris.zib.de/maven</url>
  </repository>
</repositories>
{% endhighlight %}

#### The Java-client cannot connect to Scalaris. What is wrong?

If you do not use our client script (java-api/scalaris) your hostname configuration probably needs some adjustments. Erlang and Java use different mechanisms to determine the full qualified domain name (FQDN), but both have to come to the same result in order to be able to connect.

You may check your installation using:

{% highlight sh %}
scalarisctl checkinstallation
{% endhighlight %}

which will guide you further.

  * For getting the node name Erlang uses, call:<br>
`erl -noinput -name boot -eval 'io:format("~s~n", [node()]), halt().'`<br>
If this gives you a strange erlang error, replace "boot" in the line above with some arbitrary name.
  * For getting the hostname the Java-API uses, call:
    * installed with an RPM:<br>
      `scalaris --noerl -lh`
    * inside the source directory:<br>
      `./java-api/scalaris --noconfig --noerl -lh`

The domain name from Erlang should be the same as the name printed by the Java client. If not, you may need to change your `/etc/hosts` or `/etc/hostname` for it to work.

### Python-API

#### Reading a list of (small) integers results in a unicode object instead of a list.

Unfortunately, Erlang can not distinguish between list of (small) integers and strings (which are lists of small integers, internally). Therefore, we can not guess the resulting type neither in the Java API, nor in the Python API.
The Java API contains several ???Value() methods for the values read back from
Scalaris.
Since python is dynamically typed, most of these are not needed except for the
conversion from (falsely interpreted) strings to lists of integers. Use our `str_to_list()` utility method form the `Scalaris` module for this task.

Note: You can safely use `Scalaris.str_to_list()` for any integer list (even those with large integers which are already returned as valid lists). It will return the original value if it is not a string or unicode object.

## Known Issues

### Publishing topics with Erlang R13B01 may fail

The httpc_handler module of the R13B01 distribution sporadically crashes when a message is published to a web server. This can be seen using our integrated java tests. There have been some changes to that module in R13B02 which seem to have fixed this bug. We haven't observed this behavior in Erlang R13B02 - R14A.

### Configure claims that the Java client script may not work

Our client script wrapper in `java-api/scalaris` uses `java-functions` and `build-classpath` to cope with distribution-specific paths. If either of them is not present, the client script _may_ not work. The following explains a bit more what they do and how they are used:

  * `java-functions` (if present) will be used to setup the java environment before starting the client
  * `build-classpath` is a script that eases classpath creation especially when a script is being used on different platforms. If `build-classpath` is not present, our wrapper script will try to locate dependent jar files in `$SCALARIS_JAVA_HOME/lib` - these jars include `OtpErlang-X.X.X.jar` and `jakarta-commons-cli-X.X.jar` which can be copied from `./java-api/lib/`. If unset, SCALARIS_JAVA_HOME will be set to the client wrapper script's location which enables the script to be called from a checkout of the scalaris sources.
