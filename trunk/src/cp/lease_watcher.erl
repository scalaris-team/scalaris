% @copyright 2007-2014 Zuse Institute Berlin

%  Licensed under the Apache License, Version 2.0 (the "License");
%  you may not use this file except in compliance with the License.
%  You may obtain a copy of the License at
%
%      http://www.apache.org/licenses/LICENSE-2.0
%
%  Unless required by applicable law or agreed to in writing, software
%  distributed under the License is distributed on an "AS IS" BASIS,
%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%  See the License for the specific language governing permissions and
%  limitations under the License.

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Watching rbr for timed out leases.
%% @end
%% @version $$
-module(lease_watcher).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).
-include("scalaris.hrl").
-include("record_helpers.hrl").

%% gen_component callbacks
-export([start_link/1, init/1, on/2]).

-type state() :: ok.

%% gen_component functions
%% @doc Starts the lease watcher server
-spec start_link(pid_groups:groupname()) -> {ok, pid()} | ignore.
start_link(DHTNodeGroup) ->
    case config:read(leases) of
        true ->
            gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                                     [{wait_for_init}, {erlang_register, ?MODULE},
                                      {pid_groups_join_as, DHTNodeGroup, ?MODULE}]);
        _ ->
            ignore
    end.

%% @doc Initialises the module with an empty state.
-spec init([]) -> state().
init([]) ->
    ok.

%% @private
-spec on(comm:message(), state()) -> state().
on({trigger}, State) ->
    DHTNode = pid_groups:get_my(dht_node),
    Self1 = comm:reply_as(self(), 3, {leases, 1, '_'}),
    Self2 = comm:reply_as(self(), 3, {leases, 2, '_'}),
    Self3 = comm:reply_as(self(), 3, {leases, 3, '_'}),
    Self4 = comm:reply_as(self(), 3, {leases, 4, '_'}),
    comm:send_local(DHTNode, {prbr, tab2list, leases_1, Self1}),
    comm:send_local(DHTNode, {prbr, tab2list, leases_2, Self2}),
    comm:send_local(DHTNode, {prbr, tab2list, leases_3, Self3}),
    comm:send_local(DHTNode, {prbr, tab2list, leases_4, Self4}),
    State;

on({leases, _Segment, {_DB, List}}, State) ->
    %log:log("got leases ~w ~w ~w~n", [_Segment, _DB, List]),
    LeasesToRevive = filter(List),
    [ process_lease(L) || {_Id, L} <- LeasesToRevive],
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec filter([{?RT:key(), l_on_cseq:lease_t()}]) -> [{?RT:key(), l_on_cseq:lease_t()}].
filter(Leases) ->
    F = fun({Id, L}) ->
                LeaseId = l_on_cseq:get_id(L),
                %log:log("~w ~w~n", [Id, L]),
                Id =:= LeaseId andalso %% is first replica?
                    l_on_cseq:is_live_aux_field(L) andalso
                    l_on_cseq:has_timed_out(L)
        end,
    lists:filter(F, Leases).

-spec process_lease(l_on_cseq:lease_t()) -> ok.
process_lease(L) ->
    MyDHTNode = comm:make_global(pid_groups:get_my(dht_node)),
    case l_on_cseq:get_aux(L) of
        empty ->
            l_on_cseq:lease_send_lease_to_node(MyDHTNode, L, passive),
            ok;
        {change_owner, Owner} ->
            l_on_cseq:lease_send_lease_to_node(Owner, L, passive),
            ok;
        {invalid, split, _R1, _R2} ->
            % @todo
            ok;
        {valid,   split, _R1, _R2} ->
            % @todo
            ok;
        {invalid, merge, _R1, _R2} ->
            % @todo
            ok;
        {valid,   merge, _R1, _R2} ->
            % @todo
            ok;
        {invalid,merge,stopped} ->
            % only happens in unit tests
            ok
    end.
