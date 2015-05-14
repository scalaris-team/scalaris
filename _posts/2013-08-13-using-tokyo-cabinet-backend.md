---
title: Tokyo Cabinet Backend
excerpt: Using Tokyo Cabinet as the backend.
layout: default
---

# {{ page.title }}

## Introduction

Tokyo Cabinet can be used to store more data in a Scalaris node than would fit into its main memory.

Note: you don't get persistence!!! DB files are truncated on first access!!!

## Prerequisites

You will need:

1. [Tokyo Cabinet](http://fallabs.com/tokyocabinet/)
2. [toke](http://hg.opensource.lshift.net/toke/)

# Switching to toke

1. Install Tokyo Cabinet
2. Install/make toke, e.g. by installing an rpm, provided by our repositories for some linux distributions
3. Rerun configure with `--enable-toke`. configure assumes that you installed toke in your erlang's lib directory, i.e. "<erlang_dir>/lib/toke" or "<erlang_dir>/lib/toke-<version>". If you use a different directory, e.g. /home/scalaris/apps/toke, you have to provide the path to configure:

    `./configure --enable-toke=/home/scalaris/apps/toke/`

4. Change the database used by db_dht to db_toke (in db_dht.erl):

    `-define(DB, db_toke).`

    `%-define(DB, db_ets).`

5. recompile

    `make`

The database files are currently stored in the data sub-directory. Each node creates its own sub-directory and each scalaris node will have a different file. The path can be changed in scalaris.cfg with the db_directory option.
