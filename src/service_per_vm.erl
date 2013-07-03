% @copyright 2010-2011 Zuse Institute Berlin

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
%% @doc Handling all scalaris nodes inside an erlang VM.
%% @version $Id$
-module(service_per_vm).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-export([dump_node_states/0, kill_nodes/1, get_round_trip/2, register_dht_node/1, deregister_dht_node/1]).

-export([start_link/1, init/1, on/2]).

% state of the module
-type state() :: list(comm:mypid_plain()).

% accepted messages the module
-type message() :: {get_dht_nodes, ReplyPid :: comm:mypid()} |
                   {register_dht_node, PidToAdd :: comm:mypid()} |
                   {deregister_dht_node, PidToRemove :: comm:mypid()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc ask all local nodes for their state
-spec dump_node_states() -> [term()].
dump_node_states() ->
    [gen_component:get_state(Pid)
     || Pid <- pid_groups:find_all(dht_node)].

-spec kill_nodes(No::non_neg_integer()) -> ok.
kill_nodes(No) ->
    Childs = lists:sublist([X || X <- supervisor:which_children(main_sup),
                                 is_list(element(1, X))], No),
    _ = [begin
             SupDhtNode = element(2, Child),
             Id = element(1, Child),
             sup:supervisor_terminate_childs(SupDhtNode),
             _ = supervisor:terminate_child(main_sup, Id),
             supervisor:delete_child(main_sup, Id)
         end || Child <- Childs],
    ok.

% @doc Sends register message to running service_per_vm
-spec register_dht_node(comm:plain_pid()) -> ok.
register_dht_node(Pid) ->
    case get_service() of
        failed -> ok;
        Service ->  comm:send_local(Service, {register_dht_node, Pid})
    end,
    ok.

% @doc Sends deregister message to running service_per_vm
-spec deregister_dht_node(comm:plain_pid()) -> ok.
deregister_dht_node(Pid) ->
    case get_service() of
        failed -> ok;
        Service ->  comm:send_local(Service, {deregister_dht_node, Pid})
    end,
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Server process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(ServiceGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{erlang_register, service_per_vm},
                              {pid_groups_join_as, ServiceGroup, ?MODULE}]).

-spec init(any()) -> state().
init(_Arg) ->
    [].

-spec on(Message :: message(), State :: state()) -> state().

% @doc registers a dht node
on({register_dht_node, Pid}, State) ->
    [comm:make_global(Pid) | State];

% @doc de-registers a dht node
on({deregister_dht_node, Pid}, State) ->
    [Node || Node <- State, Node /= comm:make_global(Pid)];

% @doc replies with the list of registered dht nodes
on({get_dht_nodes, Pid}, State) ->
    case comm:is_valid(Pid) of
        true ->
            comm:send(Pid, {get_dht_nodes_response, State});
        false ->
            ok
    end,
    State;

on({delete_node, SupPid, SupId}, State) ->
    sup:supervisor_terminate_childs(SupPid),
    _ = supervisor:terminate_child(main_sup, SupId),
    _ = supervisor:delete_child(main_sup, SupId),
    State;

% message from comm:init_and_wait_for_valid_pid/0 (no reply needed)
on({hi}, State) ->
    State.

-spec get_round_trip(GPid::comm:mypid(), Iterations::pos_integer()) -> float().
get_round_trip(GPid, Iterations) ->
    Start = erlang:now(),
    get_round_trip_helper(GPid, Iterations),
    End = erlang:now(),
    timer:now_diff(End, Start) / Iterations.

-spec get_round_trip_helper(GPid::comm:mypid(), Iterations::pos_integer()) -> ok.
get_round_trip_helper(_GPid, 0) ->
    ok;
get_round_trip_helper(GPid, Iterations) ->
    comm:send(GPid, {ping, comm:this()}),
    receive _Any -> ok end,
    get_round_trip_helper(GPid, Iterations - 1).

-spec get_service() -> comm:mypid() | failed.
get_service() ->
    pid_groups:find_a(service_per_vm).
