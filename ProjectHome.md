## Scalaris, a distributed transactional key-value store ##

Scalaris is a scalable, transactional, distributed key-value store. It was the first NoSQL database, that supported the ACID properties for multi-key transactions. It can
be used for building scalable Web 2.0 services.

Scalaris uses a structured overlay with a non-blocking Paxos commit protocol
for transaction processing with strong consistency over replicas. Scalaris
is implemented in Erlang.

Documentation / Download / Discussion:
  * [Users and Developers Guide](https://docs.google.com/gview?url=http://scalaris.googlecode.com/svn/trunk/user-dev-guide/main.pdf) (download as [pdf](http://scalaris.googlecode.com/svn/trunk/user-dev-guide/main.pdf))
  * [FAQ](FAQ.md)
  * [Downloads](http://sourceforge.net/projects/scalaris/files/)
  * [Prebuild packages](http://code.google.com/p/scalaris/wiki/FAQ#Prebuild_packages)
  * [Mailing list](http://groups.google.com/group/scalaris)

The Scalaris project was initiated and is mainly developed by [Zuse Institute Berlin](http://www.zib.de/PVS). It received funding from the EU projects Selfman, XtreemOS, 4CaaSt, Contrail, and IES Cities.
More information (papers, videos) can be found [here](http://www.zib.de/node/756) and [here](http://www.onscale.de/scalarix.html).

![http://www.zib.de/schuett/scalaris/ies-cities-40.png](http://www.zib.de/schuett/scalaris/ies-cities-40.png)
![http://www.zib.de/schuett/scalaris/4caast-40.png](http://www.zib.de/schuett/scalaris/4caast-40.png)
![http://www.zib.de/schuett/scalaris/contrail-40.png](http://www.zib.de/schuett/scalaris/contrail-40.png)
![http://www.zib.de/schuett/scalaris/selflogo-40.jpg](http://www.zib.de/schuett/scalaris/selflogo-40.jpg)
![http://www.zib.de/schuett/scalaris/xtreemos-40.png](http://www.zib.de/schuett/scalaris/xtreemos-40.png)
![http://www.zib.de/schuett/scalaris/ist-40.gif](http://www.zib.de/schuett/scalaris/ist-40.gif)
![http://www.zib.de/schuett/scalaris/eu-40.png](http://www.zib.de/schuett/scalaris/eu-40.png)
![http://www.zib.de/schuett/scalaris/ziblogo-40.gif](http://www.zib.de/schuett/scalaris/ziblogo-40.gif)

## Current Stable Release ##

### Scalaris 0.7.2 - October 23, 2014 ###
(partly supported by the EU project IES Cities http://iescities.eu/
and the EIT ICT Labs project MCData)

#### Packaging ####

  * fix ArchLinux packages with newest Java versions

#### Demonstrator "Wiki on Scalaris" ####

  * fix the separate count key optimisation not using Scalaris' increment operation

#### Business Logic ####

  * rrepair: let the trivial algorithm assume the worst case in order to always meet the configured "recon probability of one error" (p1e)
  * rrepair: fix the trivial algorithm having an effectively doubled p1e
  * rrepair: fix the bloom algorithm having an effectively tripled p1e
  * rrepair: allow disabling byte-alignment

#### Bugs ####

  * fix a few minor bugs

### [Previous Releases](PreviousReleases.md) ###