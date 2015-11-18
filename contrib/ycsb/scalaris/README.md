Scalaris YCSB Client
====================

The Scalaris client is not yet part of the official YCSB bundle but a fork of
YCSB with the necessary implementation is available on GitHub.

Compile
-------

Check out the fork of YCSB with branch scalaris:

	git clone git://github.com/mxm/YCSB.git -b scalaris
  
Build with Maven:

	cd YCSB
	mvn package

Run
---

	./bin/ycsb load scalaris -P workloads/workloada
	./bin/ycsb run scalaris -P workloads/workloada

Troubleshooting
---------------

If you're experiencing problems, first make sure Scalaris is running. You can pass configuration parameters to YCBS. For instance, if your first Scalaris node is node1@localhost, the configuration looks as follows:

	./bin/ycsb load scalaris -P workloads/workloada -p scalaris.node="node1@localhost"

The following parameters with their default values are available:

	scalaris.node = "node1@localhost"
	scalaris.name = "YCSB"
	scalaris.cookie = "chocolate chip cookie"
