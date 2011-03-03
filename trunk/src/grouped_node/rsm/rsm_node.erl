%  @copyright 2011 Zuse Institute Berlin

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
%% @doc    node in a rsm
%% @end
%% @version $Id$
-module(rsm_node).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/3]).

-export([on/2, on_joining/2, init/1]).

-export([is_alive/1]).

-type(message() :: any()).

%% @doc message handler for join protocol
-spec on_joining(message(), rsm_state:state()) -> rsm_state:state().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% join protocol
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on_joining({rsm_state, State}, _State) ->
    % @todo: assert that I am a member of this group
    View = rsm_state:get_view(State),
    Members = rsm_view:get_members(View),
    fd:subscribe(Members),
    gen_component:change_handler(State, on);
on_joining({rsm_join_response, retry, _Reason}, State) ->
    % retry
    comm:send_local_after(500, self(), {join_timeout}),
    RSMPid = rsm_state:get_rsm_pid(State),
    send_all(RSMPid, {join_request, comm:this()}),
    group_state:set_mode(State, joining);
on_joining({rsm_join_response,is_already_member}, State) ->
    %io:format("got is_already_member on joining~n", []),
    %@todo ?
    State;
on_joining({join_timeout}, State) ->
    RSMPid = rsm_state:get_rsm_pid(State),
    Acceptor = comm:make_global(pid_groups:get_my(paxos_acceptor)),
    Learner = comm:make_global(pid_groups:get_my(paxos_learner)),
    send_all(RSMPid, {join_request, comm:this(), Acceptor, Learner}),
    comm:send_local_after(500, self(), {join_timeout}),
    State;
on_joining({state_update, State}, _State) ->
    View = rsm_state:get_view(State),
    NewView = rsm_view:recalculate_index(View),
    NewView2 = rsm_paxos_utils:init_paxos(NewView),
    Members = rsm_view:get_members(NewView2),
    fd:subscribe(Members),
    NewState = rsm_state:set_mode(
                   rsm_state:set_view(State, NewView2),
                 joined),
    gen_component:change_handler(NewState, on).

-spec on(message(), rsm_state:state()) -> rsm_state:state().

% rsm protocol (total ordered broadcast)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% rsm_node_join
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%on({rsm_node_join, Pid, Acceptor, Learner}, State) ->
%    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% rsm_node_remove
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%on({rsm_node_remove, DeadPid, Proposer}, State) ->
%    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% paxos decided
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({learner_decide, _Cookie, PaxosId, Proposal}, State) ->
    rsm_tob:deliver(PaxosId, Proposal, State);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% retry
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({rsm_join_response,retry}, State) ->
    % @todo: do nothing? well, we are already in the group.
    State;
on({rsm_join_response,is_already_member}, State) ->
    % @todo: do nothing? well, we are already in the group.
    State;
on({rsm_remove_response, is_no_member, _DeadPid}, State) ->
    State;
on({rsm_remove_response, retry, DeadPid}, State) ->
    View = rsm_state:get_view(State),
    case rsm_view:is_member(View, DeadPid) of
        true ->
            comm:send_local(self(), {ops, {rsm_remove, DeadPid, comm:this()}}),
            State;
        false ->
            State
    end;
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% requests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({join_request, Pid, Acceptor, Learner}, State) ->
    rsm_ops_add_node:ops_request(State, {add_node, Pid, Acceptor, Learner});
on({deliver_to_rsm_request, Message, Proposer}, State) ->
    rsm_ops_deliver:ops_request(State, {deliver, Message, Proposer});
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({join_timeout}, State) ->
    State;
on({rsm_get_single_state, Pid}, State) ->
    View = rsm_state:get_view(State),
    AppState = rsm_state:get_app_state(State),
    comm:send(Pid, {rsm_get_single_state_response, View, AppState}),
    State.

%% @doc joins this node in the rsm and calls the main loop
-spec init({list(), module()}) -> rsm_state:state() | {'$gen_component', list(), rsm_state:state()}.
init({Option, AppModule}) ->
    case Option of
        first ->
            Acceptor = comm:make_global(pid_groups:get_my(paxos_acceptor)),
            Learner = comm:make_global(pid_groups:get_my(paxos_learner)),
            View = rsm_paxos_utils:init_paxos(rsm_view:new_primary(Acceptor, Learner)),
            rsm_state:new_primary(AppModule, View);
        {join, RSMPid} ->
            Acceptor = comm:make_global(pid_groups:get_my(paxos_acceptor)),
            Learner = comm:make_global(pid_groups:get_my(paxos_learner)),
            send_all(RSMPid, {join_request, comm:this(), Acceptor, Learner}),
            comm:send_local_after(500, self(), {join_timeout}),
            % starts with on-joining-handler
            gen_component:change_handler(rsm_state:new_replica(AppModule, RSMPid), on_joining)
    end.

-spec start_link(pid_groups:groupname(), list(), module()) -> {ok, pid()}.
start_link(GroupName, Options, AppModule) ->
    gen_component:start_link(?MODULE, {Options, AppModule},
                             [{pid_groups_join_as, GroupName, rsm_node},
                              wait_for_init]).

-spec is_alive(pid()) -> boolean().
is_alive(Pid) ->
    try
        rsm_state:get_mode(gen_component:get_state(Pid)) =:= joined
    catch _:_ -> false
    end.

-spec send_all(rsm_state:rsm_pid_type(), any()) -> ok.
send_all(RSMPid, Msg) when is_list(RSMPid) ->
   [comm:send(P, Msg) || P <- RSMPid],
    ok.
