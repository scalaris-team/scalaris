% @copyright 2007-2015 Zuse Institute Berlin

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
%% @version $Id$
-module(mgmt_server).
-author('schuett@zib.de').
-vsn('$Id$').

-export([start_link/2,
         number_of_nodes/0,
         node_list/0, node_list/1]).

-behaviour(gen_component).
-include("scalaris.hrl").

-export([init/1, on/2]).

-include("gen_component.hrl").

% accepted messages of the mgmt_server process
-type(message() ::
    {fd_notify, fd:event(), PID::comm:mypid(), Reason::fd:reason()} |
    {get_list, SourcePid::comm:mypid()} |
    {get_list_length, SourcePid::comm:mypid()} |
    {register, Node::node:node_type()}).

% internal state (known nodes)
-type(state()::Nodes::gb_trees:tree(comm:mypid(), node:node_type())).

%% @doc trigger a message with the number of nodes known to the mgmt server
-spec number_of_nodes() -> ok.
number_of_nodes() ->
    Pid = mgmtPid(),
    This = comm:this(),
    case comm:is_valid(Pid) andalso comm:is_valid(This) of
        true -> comm:send(Pid, {get_list_length, This});
        _    -> comm:send_local(self(), {get_list_length_response, 0})
    end.

%% @doc trigger a message with all nodes known to the mgmt server
-spec node_list() -> ok.
node_list() -> node_list(false).

-spec node_list(UseShepherd::boolean()) -> ok.
node_list(UseShepherd) ->
    Pid = mgmtPid(),
    case comm:is_valid(Pid) of
        true -> comm:send(Pid, {get_list, comm:this()},
                          ?IIF(UseShepherd, [{shepherd, self()}], []));
        _    -> comm:send_local(self(), {get_list_response, []})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Implementation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(message(), state()) -> state().
on({fd_notify, crash, PID, _Reason}, Nodes) ->
    case gb_trees:lookup(PID, Nodes) of
        {value, Node} -> dn_cache:add_zombie_candidate(Node),
                         gb_trees:delete(PID, Nodes);
        none          -> Nodes
    end;
on({fd_notify, leave, PID, _Reason}, Nodes) ->
    % graceful leave - prevent the node being added as a zombie candidate by removing now:
    gb_trees:delete_any(PID, Nodes);
on({fd_notify, _Event, _PID, _Reason}, Nodes) ->
    Nodes;

on({get_list, SourcePid}, Nodes) ->
    comm:send(SourcePid, {get_list_response, gb_trees:keys(Nodes)}),
    Nodes;

on({get_list_length, SourcePid}, Nodes) ->
    comm:send(SourcePid, {get_list_length_response, gb_trees:size(Nodes)}),
    Nodes;

on({register, Node}, Nodes) ->
    NodePid = node:pidX(Node),
    case gb_trees:lookup(NodePid, Nodes) of
        {value, _OldNode} -> gb_trees:update(NodePid, Node, Nodes);
        none              -> fd:subscribe(self(), [NodePid]),
                             gb_trees:insert(NodePid, Node, Nodes)
    end;

% dead-node-cache reported dead node to be alive again
on({zombie, Node}, Nodes) ->
    on({register, Node}, Nodes);

on({web_debug_info, Requestor}, Nodes) ->
    RegisteredPids = gb_trees:keys(Nodes),
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
    dn_cache:subscribe(),
    gb_trees:empty().

%% @doc starts the server; called by the mgmt supervisor
%% @see sup_scalaris
-spec start_link(pid_groups:groupname(), [tuple()]) -> {ok, pid()}.
start_link(ServiceGroup, Options) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, Options,
                             [{erlang_register, mgmt_server},
                              {pid_groups_join_as, ServiceGroup, ?MODULE}]).

%% @doc pid of the mgmt server (may be invalid)
-spec mgmtPid() -> comm:mypid() | any().
mgmtPid() ->
    config:read(mgmt_server).
