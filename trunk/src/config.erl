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
%% @version $Id: config.erl 494 2008-07-04 17:07:34Z schintke $
-module(config).

-author('schuett@zib.de').
-vsn('$Id: config.erl 494 2008-07-04 17:07:34Z schintke $ ').

-behaviour(gen_server).

-export([start_link/2, init/1, handle_call/3, handle_cast/2, 
	 handle_info/2, code_change/3, terminate/2, stop/0,

	 read/1,

	 succListLength/0, stabilizationInterval/0, 
	 pointerStabilizationInterval/0, failureDetectorInterval/0, 
	 failureDetectorPingInterval/0, 
	 failureDetectorUpdateInterval/0, bootPid/0, 
	 logPid/0, loadBalanceInterval/0, loadBalanceStartupInterval/0, loadBalanceFlagResetInterval/0, 
	 collectorInterval/0, error_log_file/0, rrd_log_file/0, 
	 debugQueueLength/0, debug_log_file/0, transaction_log_file/0, reregisterInterval/0, 
	 replicationFactor/0, quorumFactor/0, replicaPrefixes/0, 
	 transactionLookupTimeout/0, tpFailureTimeout/0, 
	 tmanagerTimeout/0,
	 readTimeout/0, leaderDetectorInterval/0, 
	 testDump/0, testKeepAlive/0, register_hosts/0, listenPort/0, listenIP/0]).

%%====================================================================
%% public functions
%%====================================================================

get_pid() ->
    InstanceId = erlang:get(instance_id),
    if
	InstanceId == undefined ->
	    io:format("missing instance_id: ~p~n", [util:get_stacktrace()]);
	true ->
	    ok
    end,
    process_dictionary:lookup_process(InstanceId, config).

%% @doc read config parameter
%% @spec read(term()) -> term() | failed
read(Key) ->
    gen_server:call(get_pid(), {read, Key}, 20000).

%%====================================================================
%% public functions
%%====================================================================

%% @doc the length of the successor list
%% @spec succListLength() -> integer() | failed
succListLength() ->
    gen_server:call(get_pid(), {read, succ_list_length}, 20000).


%% @doc the interval between two failure detection runs
%% @spec failureDetectorInterval() -> integer() | failed
failureDetectorInterval() ->
    gen_server:call(get_pid(), {read, failure_detector_interval}, 20000).

%% @doc the interval between two failure detection runs
%% @spec failureDetectorPingInterval() -> integer() | failed
failureDetectorPingInterval() ->
    gen_server:call(get_pid(), {read, failure_detector_ping_interval}, 20000).

%% @doc the interval between two stabilization runs
%% @spec stabilizationInterval() -> integer() | failed
stabilizationInterval() ->
    gen_server:call(get_pid(), {read, stabilization_interval}, 20000).

%% @doc the interval between two finger/pointer stabilization runs
%% @spec pointerStabilizationInterval() -> integer() | failed
pointerStabilizationInterval() ->
    gen_server:call(get_pid(), {read, pointer_stabilization_interval}, 20000).

%% @doc interval between two updates of the nodes to be supervised 
%% @spec failureDetectorUpdateInterval() -> integer() | failed
failureDetectorUpdateInterval() ->
    gen_server:call(get_pid(), {read, failure_detector_update_interval}, 20000).

%% @doc interval between two load balance rounds
%% @spec loadBalanceInterval() -> integer() | failed
loadBalanceInterval() ->
    gen_server:call(get_pid(), {read, load_balance_interval}, 20000).

%% @doc interval between two load balance rounds
%% @spec loadBalanceStartupInterval() -> integer() | failed
loadBalanceStartupInterval() ->
    gen_server:call(get_pid(), {read, load_balance_startup_interval}, 20000).

%% @doc interval between two flag reset events
%% @spec loadBalanceFlagResetInterval() -> integer() | failed
loadBalanceFlagResetInterval() ->
    gen_server:call(get_pid(), {read, load_balance_flag_reset_interval}, 20000).

%% @doc hostname of the boot daemon
%% @spec bootHost() -> string() | failed
bootHost() ->
    gen_server:call(get_pid(), {read, boot_host}, 20000).

%% @doc pid of the boot daemon
%% @spec bootPid() -> pid()
bootPid() ->
    bootHost().
    %{boot, bootHost()}.

%% @doc pid of the log daemon
%% @spec logPid() -> pid()
logPid() ->
    gen_server:call(get_pid(), {read, log_host}, 20000).

%% @doc interval between two collections of the message statistics
%% @spec collectorInterval() -> integer() | failed
collectorInterval() ->
    gen_server:call(get_pid(), {read, collector_interval}, 20000).

%% @doc path to the log directory
%% @spec log_path() -> string()
log_path() ->
    {ok, CWD} = file:get_cwd(),
    CWD ++ "/../log/".

%% @doc path to the error log file
%% @spec error_log_file() -> string()
error_log_file() ->
    string:concat(log_path(), "error_log.txt").

%% @doc path to the rrdtools log file
%% @spec rrd_log_file() -> string()
rrd_log_file() ->
    string:concat(log_path(), "rrd_log.txt").

%% @doc path to the debug log file
%% @spec debug_log_file() -> string()
debug_log_file() ->
    string:concat(log_path(), "debug_log.txt").

%% @doc path to the transaction log file
%% @spec transaction_log_file() -> string()
transaction_log_file() ->
    string:concat(log_path(), "transaction_log.txt").

%% @doc length of the debug queue
%% @spec debugQueueLength() -> integer() | failed
debugQueueLength() ->
    gen_server:call(get_pid(), {read, debug_queue_length}, 20000).

%% @doc interval between two re-registrations with the boot daemon
%% @spec reregisterInterval() -> integer() | failed
reregisterInterval() ->
    gen_server:call(get_pid(), {read, reregister_interval}, 20000).

%% @doc the replication degree of the system
%% @spec replicationFactor() -> integer() | failed
replicationFactor() ->
    gen_server:call(get_pid(), {read, replication_factor}, 20000).

%% @doc number of nodes needed for a quorum
%% @spec quorumFactor() -> integer() | failed
quorumFactor() ->
    gen_server:call(get_pid(), {read, quorum_factor}, 20000).

%% @doc prefixes used for the replicas
%% @spec replicaPrefixes() -> [integer()] | failed
replicaPrefixes() ->
    gen_server:call(get_pid(), {read, replica_prefixes}, 20000).

%% @doc transaction node lookup timeout
%% @spec transactionLookupTimeout() -> integer() | failed
transactionLookupTimeout()->
    gen_server:call(get_pid(), {read, transaction_lookup_timeout}, 20000).

%% @doc time out for read operations
%% @spec readTimeout() -> integer() | failed
readTimeout()->
    gen_server:call(get_pid(), {read, read_timeout}, 20000).

tpFailureTimeout()->
    gen_server:call(get_pid(), {read, tp_failure_timeout}, 20000).

%% @doc transaction leader detection interval
%% @spec leaderDetectorInterval() -> integer() | failed
leaderDetectorInterval()->
    gen_server:call(get_pid(), {read, leader_detector_interval}, 20000).
    
tmanagerTimeout()->
    gen_server:call(get_pid(), {read, tmanager_timeout}, 20000).

testDump()->
	gen_server:call(get_pid(), {read, test_dump}, 20000).

testKeepAlive()->
	gen_server:call(get_pid(), {read, test_keep_alive}, 20000).

%% @doc with which nodes to register regularly, alternative to boot_host
%% @spec register_hosts() -> list(pid()) | failed
register_hosts()->
    %lists:map(fun (Host) -> {boot, Host} end, gen_server:call(get_pid(), {read, register_hosts}, 20000)).
    case gen_server:call(get_pid(), {read, register_hosts}, 20000) of
	failed ->
	    failed;
	Nodes ->
	    lists:map(fun (Host) -> {boot, Host} end, Nodes)
    end,
    gen_server:call(get_pid(), {read, register_hosts}, 20000).

%% @doc port to listen on for TCP
%% @spec listenPort() -> int()
listenPort()->
	gen_server:call(get_pid(), {read, listen_port}, 20000).

%% @doc IP to listen on for TCP
%% @spec listenIP() -> inet:ip_address() | undefined
listenIP()->
	gen_server:call(get_pid(), {read, listen_ip}, 20000).

%%====================================================================
%% gen_server setup
%%====================================================================

start_link(Files, InstanceId) ->
    io:format("Config files: ~p~n", [Files]),
    %{local, ?MODULE}, ; Files, InstanceId
    gen_server:start_link(?MODULE, [Files, InstanceId], []).

%@private
%init([X, InstanceId]) ->
%    {ok, ok};

init([[File], InstanceId]) ->
%    io:format("starting config~n", []),
    process_dictionary:register_process(InstanceId, config, self()),
    {ok, populate_db(gb_trees:empty(), File)};

%@private
init([[Global, Local], InstanceId]) ->
%    io:format("starting config~n", []),
    process_dictionary:register_process(InstanceId, config, self()),
    {ok, populate_db(populate_db(gb_trees:empty(), Global), 
		     Local)}.

%@private
stop() ->
    gen_server:cast(?MODULE, stop).

populate_db(Dictionary, File) ->
    case file:consult(File) of
	{ok, Terms} ->
	    eval_environment(lists:foldl(fun process_term/2, Dictionary, Terms), os:getenv("CS_PORT"));
	    %lists:foldl(fun process_term/2, Dictionary, Terms);
	{error, Reason} ->
	    io:format("Can't load config file ~p: ~p. Ignoring.\n", [File, Reason]), Dictionary
	    % exit(file:format_error(Reason))
    end.

eval_environment(Dictionary, false) ->
    Dictionary;
eval_environment(Dictionary, Port) ->
    {PortInt, []} = string:to_integer(Port),
    gb_trees:enter(listen_port, PortInt, Dictionary).
    
    
process_term({Key, Value}, Dictionary) ->
    gb_trees:enter(Key, Value, Dictionary).

%%====================================================================
%% gen_server callbacks
%%====================================================================

% read
%@private
handle_call({read, Key}, _From, DB) ->
    case gb_trees:lookup(Key, DB) of
	{value, Value} ->
	    {reply, Value, DB};
	none ->
	    {reply, failed, DB}
    end;

% write
%@private
handle_call({write, Key, Value}, _From, DB) ->
    NewDB = gb_trees:enter(Key, Value, DB),
    {reply, ok, NewDB}.



%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%@private
handle_cast(stop, DB) ->
    {stop, normal, DB};

%@private
handle_cast({debug_info, Requestor}, DB) ->
    Requestor ! {debug_info_response, [{"config_items", gb_trees:size(DB)}]},
    {noreply, DB};

handle_cast(_Msg, DB) ->
    {noreply, DB}.

%@private
handle_info(_Info, DB) ->
    {noreply, DB}.


%@private
code_change(_OldVsn, DB, _Extra) ->
    {ok, DB}.

%@private
terminate(_Reason, _DB) ->
    ok.

