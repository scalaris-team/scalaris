%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%
%   Licensed under the Apache License, Version 2.0 (the "License");
%   you may not use this file except in compliance with the License.
%   You may obtain a copy of the License at
%
%       http://www.apache.org/licenses/LICENSE-2.0
%
%   Unless required by applicable law or agreed to in writing, software
%   distributed under the License is distributed on an "AS IS" BASIS,
%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%   See the License for the specific language governing permissions and
%   limitations under the License.
%%%-------------------------------------------------------------------
%%% File    : config.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : config file for chord# and the bootstrapping service
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(config).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([
	 start_link/1, start/2,

	 read/1,

	 succListLength/0, stabilizationInterval_max/0, stabilizationInterval_min/0,stabilizationInterval/0,
	 pointerStabilizationInterval/0, failureDetectorInterval/0, 
	 failureDetectorPingInterval/0, 
	 failureDetectorUpdateInterval/0, bootPid/0, 
	 logPid/0, loadBalanceInterval/0, loadBalanceStartupInterval/0, loadBalanceFlagResetInterval/0, 
	 collectorInterval/0, error_log_file/0, 
	 debugQueueLength/0, debug_log_file/0, transaction_log_file/0, reregisterInterval/0, 
	 replicationFactor/0, quorumFactor/0, replicaPrefixes/0, 
	 transactionLookupTimeout/0, tpFailureTimeout/0, 
	 tmanagerTimeout/0,
	 readTimeout/0, leaderDetectorInterval/0, 
	 testDump/0, testKeepAlive/0, register_hosts/0, listenPort/0, listenIP/0,
	 knownHosts/0]).

-export([log_path/0,cs_log_file/0,log_log_file/0,mem_log_file/0,docroot/0]).

-export([storage_path/0,storage_size/0,storage_clean/0]).


%%====================================================================
%% public functions
%%====================================================================

%% @doc read config parameter
%% @spec read(term()) -> term() | failed
read(Key) ->
    case ets:lookup(config_ets, Key) of
	[{Key, Value}] ->
	    %% allow values defined as application environments to override
	    Value;
	[] ->
	    case preconfig:get_env(Key, failed) of
		failed -> failed;
		X ->
		    ets:insert(config_ets, {Key, X}),
		    X
	    end
    end.

%%====================================================================
%% public functions
%%====================================================================

%% @doc the length of the successor list
%% @spec succListLength() -> integer() | failed
succListLength() ->
    read(succ_list_length).

%% @doc the interval between two failure detection runs
%% @spec failureDetectorInterval() -> integer() | failed
failureDetectorInterval() ->
    read(failure_detector_interval).

%% @doc the interval between two failure detection runs
%% @spec failureDetectorPingInterval() -> integer() | failed
failureDetectorPingInterval() ->
    read(failure_detector_ping_interval).

%% @doc the interval between two stabilization runs Max
%% @spec stabilizationInterval() -> integer() | failed
stabilizationInterval() ->
    read(stabilization_interval_max).

%% @doc the interval between two stabilization runs Max
%% @spec stabilizationInterval_max() -> integer() | failed
stabilizationInterval_max() ->
    read(stabilization_interval_max).

%% @doc the interval between two stabilization runs Min
%% @spec stabilizationInterval_min() -> integer() | failed
stabilizationInterval_min() ->
    read(stabilization_interval_min).

%% @doc the interval between two finger/pointer stabilization runs
%% @spec pointerStabilizationInterval() -> integer() | failed
pointerStabilizationInterval() ->
    read(pointer_stabilization_interval).

%% @doc interval between two updates of the nodes to be supervised 
%% @spec failureDetectorUpdateInterval() -> integer() | failed
failureDetectorUpdateInterval() ->
    read(failure_detector_update_interval).

%% @doc interval between two load balance rounds
%% @spec loadBalanceInterval() -> integer() | failed
loadBalanceInterval() ->
    read(load_balance_interval).

%% @doc interval between two load balance rounds
%% @spec loadBalanceStartupInterval() -> integer() | failed
loadBalanceStartupInterval() ->
    read(load_balance_startup_interval).

%% @doc interval between two flag reset events
%% @spec loadBalanceFlagResetInterval() -> integer() | failed
loadBalanceFlagResetInterval() ->
    read(load_balance_flag_reset_interval).

%% @doc hostname of the boot daemon
%% @spec bootHost() -> string() | failed
bootHost() ->
    read(boot_host).

%% @doc pid of the boot daemon
%% @spec bootPid() -> pid()
bootPid() ->
    bootHost().
    %{boot, bootHost()}.

%% @doc pid of the log daemon
%% @spec logPid() -> pid()
logPid() ->
    read(log_host).

%% @doc interval between two collections of the message statistics
%% @spec collectorInterval() -> integer() | failed
collectorInterval() ->
    read(collector_interval).

%% @doc path to the log directory
%% @spec log_path() -> string()
log_path() ->
    preconfig:log_path().

%% @doc document root for the boot server yaws server
%% @spec docroot() -> string()
docroot() ->
    preconfig:docroot().

%% @doc path to the error log file
%% @spec error_log_file() -> string()
error_log_file() ->
    filename:join(log_path(), "error_log.txt").

%% @doc path to the debug log file
%% @spec debug_log_file() -> string()
debug_log_file() ->
    filename:join(log_path(), "debug_log.txt").

%% @doc path to the transaction log file
%% @spec transaction_log_file() -> string()
transaction_log_file() ->
    filename:join(log_path(), "transaction_log.txt").

%% @doc path to the chordsharp log file
%% @spec cs_log_file() -> string()
cs_log_file() ->
    filename:join(log_path(), "cs_log.txt").

%% @doc path to the logger log file
%% @spec log_log_file() -> string()
log_log_file() ->
    filename:join(log_path(), "log.txt").

%% @doc path to the mem log file
%% @spec mem_log_file() -> string()
mem_log_file() ->
    filename:join(log_path(), "mem.txt").

%% @doc path of file storage directory
%% @spec storage_path() -> string()
storage_path() ->
    read(storage_path).

%% @doc size of file storage directory
%% @spec storage_size() -> integer()
storage_size() ->
    read(storage_size).

%% @doc whether to recreate file storage from scratch
%% @spec storage_clean() -> boolean()
storage_clean() ->
    read(storage_clean).

%% @doc length of the debug queue
%% @spec debugQueueLength() -> integer() | failed
debugQueueLength() ->
    read(debug_queue_length).

%% @doc interval between two re-registrations with the boot daemon
%% @spec reregisterInterval() -> integer() | failed
reregisterInterval() ->
    read(reregister_interval).

%% @doc the replication degree of the system
%% @spec replicationFactor() -> integer() | failed
replicationFactor() ->
    read(replication_factor).

%% @doc number of nodes needed for a quorum
%% @spec quorumFactor() -> integer() | failed
quorumFactor() ->
    read(quorum_factor).

%% @doc prefixes used for the replicas
%% @spec replicaPrefixes() -> [integer()] | failed
replicaPrefixes() ->
    read(replica_prefixes).

%% @doc transaction node lookup timeout
%% @spec transactionLookupTimeout() -> integer() | failed
transactionLookupTimeout()->
    read(transaction_lookup_timeout).

%% @doc time out for read operations
%% @spec readTimeout() -> integer() | failed
readTimeout()->
    read(read_timeout).

tpFailureTimeout()->
    read(tp_failure_timeout).

%% @doc transaction leader detection interval
%% @spec leaderDetectorInterval() -> integer() | failed
leaderDetectorInterval()->
    read(leader_detector_interval).
    
tmanagerTimeout()->
    read(tmanager_timeout).

testDump()->
    read(test_dump).

testKeepAlive()->
    read(test_keep_alive).

%% @doc with which nodes to register regularly, alternative to boot_host
%% @spec register_hosts() -> list(pid()) | failed
register_hosts()->
    read(register_hosts).

%% @doc port to listen on for TCP
%% @spec listenPort() -> int()
listenPort()->
    preconfig:cs_port().

%% @doc IP to listen on for TCP
%% @spec listenIP() -> inet:ip_address() | undefined
listenIP()->
	read(listen_ip).

%% @doc known hosts
%@TODO: any() should be ip_address()
-spec(knownHosts/0 :: () -> [{any(), integer(), pid()}]).
knownHosts()->
	read(known_hosts).

%%====================================================================
%% gen_server setup
%%====================================================================

start_link(Files) ->
    io:format("Config files: ~p~n", [Files]),
    Owner = self(),
    Link = spawn_link(?MODULE, start, [Files, Owner]),
    receive
	done ->
	    ok;
	X ->
	    io:format("unknown config message  ~p", [X])
    end,
    {ok, Link}.

%@private
start([File], Owner) ->
    catch ets:new(config_ets, [set, protected, named_table]),
    populate_db(File),
    Owner ! done,
    loop();

%@private
start([Global, Local], Owner) ->
    catch ets:new(config_ets, [set, protected, named_table]),
    populate_db(Global),
    populate_db(Local),
    Owner ! done,
    loop().

loop() ->
    receive
	_ ->
	    loop()
    end.

%@private
populate_db(File) ->
    case file:consult(File) of
	{ok, Terms} ->
	    lists:map(fun process_term/1, Terms),
	    eval_environment(os:getenv("CS_PORT"));
	{error, Reason} ->
	    io:format("Can't load config file ~p: ~p. Ignoring.\n", [File, Reason]),
	    fail
    end.

eval_environment(false) ->
    ok;
eval_environment(Port) ->
    {PortInt, []} = string:to_integer(Port),
    ets:insert(config_ets, {listen_port, PortInt}).
    
process_term({Key, Value}) ->
    ets:insert(config_ets, {Key, preconfig:get_env(Key, Value)}).
