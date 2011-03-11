% @copyright 2007-2011 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc The management server maintains a list of scalaris nodes and
%%      checks their availability using a failure_detector. Its main
%%      purpose is to give new scalaris nodes a list of nodes already
%%      in the system.
%% @end
-module(mgmt_server).
-author('schuett@zib.de').
-vsn('$Id$').

-export([start_link/2,
         number_of_nodes/0,
         node_list/0,
         connect/0]).

-behaviour(gen_component).
-include("scalaris.hrl").

-export([init/1, on/2]).

% accepted messages of the mgmt_server process
-type(message() ::
    {crash, PID::comm:mypid()} |
    {get_list, Ping_PID::comm:mypid()} |
    {be_the_first, Ping_PID::comm:mypid()} |
    {get_list_length, Ping_PID::comm:mypid()} |
    {register, Ping_PID::comm:mypid()} |
    {connect}).

% internal state (known nodes)
-type(state()::Nodes::gb_set()).

%% @doc trigger a message with  the number of nodes known to the mgmt server
-spec number_of_nodes() -> ok.
number_of_nodes() ->
    comm:send(mgmtPid(), {get_list_length, comm:this()}),
    ok.

-spec connect() -> ok.
connect() ->
    % @todo we have to improve the startup process!
    comm:send(mgmtPid(), {connect}).

%% @doc trigger a message with all nodes known to the mgmt server
-spec node_list() -> ok.
node_list() ->
    comm:send(mgmtPid(), {get_list, comm:this()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Implementation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(message(), state()) -> state().
on({crash, PID}, Nodes) ->
    NewNodes = gb_sets:delete_any(PID, Nodes),
    dn_cache:add_zombie_candidate(PID),
    NewNodes;

on({get_list, Ping_PID}, Nodes) ->
    comm:send(Ping_PID, {get_list_response, gb_sets:to_list(Nodes)}),
    Nodes;

on({get_list_length, Ping_PID}, Nodes) ->
    L = length(gb_sets:to_list(Nodes)),
    comm:send(Ping_PID, {get_list_length_response, L}),
    Nodes;

on({register, Ping_PID}, Nodes) ->
    fd:subscribe(Ping_PID),
    NewNodes = gb_sets:add(Ping_PID, Nodes),
    NewNodes;

on({connect}, State) ->
    % ugly work around for finding the local ip by setting up a socket first
    State;

% dead-node-cache reported dead node to be alive again
on({zombie_pid, Ping_PID}, Nodes) ->
    fd:subscribe(Ping_PID),
    NewNodes = gb_sets:add(Ping_PID, Nodes),
    NewNodes;

on({web_debug_info, Requestor}, Nodes) ->
    RegisteredPids = gb_sets:to_list(Nodes),
    % resolve (local and remote) pids to names:
    PidNames = pid_groups:pids_to_names(RegisteredPids, 1000),
    KeyValueList =
        [{"registered nodes", length(RegisteredPids)},
         {"registered nodes (node):", ""} |
         [{"", Pid} || Pid <- PidNames]],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    Nodes.

-spec init(Options::[tuple()]) -> state().
init(_Options) ->
    case config:read(start_dht_node) of
        undefined ->
            % ugly hack to get a valid ip-address into the comm-layer
            KnownHosts = config:read(known_hosts),
            MgmtServer = config:read(mgmt_server),
            config:write(known_hosts, [MgmtServer | KnownHosts]),
            dht_node:trigger_known_nodes();
        _ -> ok
    end,
    dn_cache:subscribe(),
    gb_sets:empty().

%% @doc starts the server; called by the mgmt supervisor
%% @see sup_scalaris
-spec start_link(pid_groups:groupname(), [tuple()]) -> {ok, pid()}.
start_link(ServiceGroup, Options) ->
    gen_component:start_link(?MODULE, Options,
                             [{erlang_register, mgmt_server},
                              {pid_groups_join_as, ServiceGroup, ?MODULE}]).

%% @doc pid of the mgmt server
-spec mgmtPid() -> comm:mypid().
mgmtPid() ->
    config:read(mgmt_server).
