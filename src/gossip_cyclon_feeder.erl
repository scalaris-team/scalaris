% @copyright 2013-2015 Zuse Institute Berlin,

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

%% @author Florian Schintke <schintke@zib.de>
%% @author Nico Kruber <kruber@zib.de>
%% @author Jan Fajerski <fajerski@zib.de>
%% @doc    Finds nodes from pid_groups and known hosts and feeds them to cyclon
%% @end
%% @version $Id$
-module(gossip_cyclon_feeder).
-author('schintke@zib.de').
-author('kruber@zib.de').
-author('fajerski@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-include("scalaris.hrl").
-include("record_helpers.hrl").

-behaviour(gen_component).

-export([start_link/1, on/2, init/1]).

-include("gen_component.hrl").

-record(state, {
          local_nodes = ?required(state, local_nodes) :: [pid()],
          remote_nodes = ?required(state, remote_nodes) :: [comm:mypid_plain()],
          known_hosts = ?required(state, known_hosts) :: [comm:mypid_plain()]}).

-type state_t() :: #state{}.

-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [], 
                             [{wait_for_init},
                              {pid_groups_join_as, DHTNodeGroup, ?MODULE}]).

-spec init([]) -> state_t().
init([]) ->
    case config:read(start_type) =:= recover of
        true ->
            msg_delay:send_trigger(10, {trigger_round});
        false ->
            ok
    end,
    #state{local_nodes = pid_groups:find_all(dht_node),
           remote_nodes = [],
           known_hosts = config:read(known_hosts)
          }.

-spec on(term(), state_t()) -> state_t().
on({trigger_round}, #state{local_nodes=LocalNodes, remote_nodes=RemoteNodes, 
                           known_hosts=KnownHosts} = State) ->
    ?TRACE("gossip_cyclon_feeder:trigger: ~p ~p ~p ~n", [LocalNodes, RemoteNodes, KnownHosts]),
    % call local nodes
    [comm:send_local(Pid, {get_node_details, 
                           comm:reply_as(comm:this(), 3, 
                                         {local_node_response, Pid, '_'}), [neighbors]})
     || Pid <- LocalNodes, is_process_alive(Pid)],
    % call remote nodes
    [comm:send(Pid, {get_node_details, 
                           comm:reply_as(comm:this(), 3, 
                                         {remote_node_response, Pid, '_'}), [neighbors]},
              [{?quiet}])
     || Pid <- RemoteNodes],
    % call known hosts
    [comm:send(Pid, {get_dht_nodes, 
                           comm:reply_as(comm:this(), 3, {known_hosts_response, Pid, '_'})},
              [{?quiet}])
     || Pid <- KnownHosts],
    msg_delay:send_trigger(10, {trigger_round}),
    State;

on({local_node_response, Pid, {get_node_details_response, [{neighbors, Neighborhood}]}},
   #state{local_nodes = LocalNodes} = State) ->
    case util:lists_take(Pid, LocalNodes) of
        false ->
            %% is duplicate response
            State;
        NewLocalNodes ->
            %% @todo: notify cyclon
            notify_cyclon(Neighborhood),
            State#state{local_nodes = NewLocalNodes}
    end;

on({remote_node_response, Pid, {get_node_details_response, [{neighbors, Neighborhood}]}},
   #state{remote_nodes = RemoteNodes} = State) ->
    case util:lists_take(Pid, RemoteNodes) of
        false ->
            %% is duplicate response
            State;
        NewRemoteNodes ->
            %% @todo: notify cyclon
            notify_cyclon(Neighborhood),
            State#state{remote_nodes = NewRemoteNodes}
    end;

on({known_hosts_response, Pid, {get_dht_nodes_response, Nodes}}, 
   #state{known_hosts=KnownHosts, remote_nodes=RemoteNodes} = State) ->
    case util:lists_take(Pid, KnownHosts) of
        false ->
            %% is duplicate response
            State;
        NewKnownHosts ->
            %% add to remote_nodes
            State#state{known_hosts = NewKnownHosts,
                        remote_nodes = lists:append(Nodes, RemoteNodes)}
end.

notify_cyclon(Neighborhood) ->
    ?TRACE("gossip_cyclon_feeder:notify_cyclon: ~p ~p ~n", 
           [pid_groups:get_my(gossip), Neighborhood]),
    Pred = nodelist:pred(Neighborhood),
    Node = nodelist:node(Neighborhood),
    Succ = nodelist:succ(Neighborhood),
    comm:send_local(pid_groups:get_my(gossip),
                    {cb_msg, 
                     {gossip_cyclon, default}, 
                     {add_nodes_to_cache, [Pred, Node, Succ]}}).
