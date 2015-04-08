# Introduction #

Tokyo Cabinet can be used to store more data in a Scalaris node than would fit into its main memory.

Note: you don't get [persistence](FAQ#Is_the_store_persisted_on_disk?.md)!!! DB files are truncated on first access!!!

# Prerequisites #

You will need:
  * [Tokyo Cabinet](http://fallabs.com/tokyocabinet/)
  * [toke](http://hg.opensource.lshift.net/toke/)

# Switching to toke #

  1. Install Tokyo Cabinet
  1. Install/make toke, e.g. by installing an rpm, provided by our [repositories](FAQ#Prebuild_packages.md) for some linux distributions
  1. Rerun configure with --enable-toke. configure assumes that you installed toke in your erlang's _lib_ directory, i.e. "`<erlang_dir`>/lib/toke" or "`<erlang_dir`>/lib/toke-`<version`>". If you use a different directory, e.g. /home/scalaris/apps/toke, you have to provide the path to configure:
```
./configure --enable-toke=/home/scalaris/apps/toke/
```
  1. Change the database used by db\_dht to db\_toke (in `db_dht.erl`):
```
-define(DB, db_toke).
%-define(DB, db_ets).
```
  1. recompile
```
make
```

The database files are currently stored in the data sub-directory. Each node creates its own sub-directory and each scalaris node will have a different file. The path can be changed in scalaris.cfg with the _db\_directory_ option.