%  @copyright 2009-2014 Zuse Institute Berlin

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
%% @doc Vivaldi helper module for measuring latency between nodes.
%% @end
%% @version $$
-module(mockup_l_on_cseq).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").
-include("record_helpers.hrl").

-export([on/2, init/1, start_link/0]).

% lease mgmt.
-export([create_two_adjacent_leases/0]).
-export([create_lease/2]).

% public api
-export([get_renewal_counter/0, get_lease_list/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec create_two_adjacent_leases() -> {l_on_cseq:lease_t(), l_on_cseq:lease_t()}.
create_two_adjacent_leases() ->
    % we have a symmetric ring with four nodes, so we are going to use
    % the ranges: 0-1 and 1-2.
    Key1 = rt_SUITE:number_to_key(0),
    Key2 = rt_SUITE:number_to_key(2),
    SplitKey = ?RT:get_split_key(Key1, Key2, {1, 2}),
    L1 = create_lease(Key1, SplitKey),
    L2 = create_lease(SplitKey, Key2),
    ct:pal("mock leases~n~w~n~w~n", [L1, L2]),
    {L1, L2}.

-spec create_lease(?RT:key(), ?RT:key()) -> l_on_cseq:lease_t().
create_lease(From, To) ->
    L = l_on_cseq:unittest_create_lease_with_range(From, To),
    Range = node:mk_interval_between_ids(From, To),
    Id = l_on_cseq:id(Range),
    DB = l_on_cseq:get_db_for_id(Id),
    rbrcseq:qwrite(DB, self(), Id,
                   fun (_Current, _WriteFile, _Next) ->
                           {true, null}
                   end,
                   L),
    receive
        X -> ct:log("qwrite~n~w~n", [X])
    end,
    %trace_mpath:thread_yield(),
    %receive
    %    ?SCALARIS_RECV(
    %       {mock, ok}, %% ->
    %       ok
    %      )
    %end,
    L.

-spec get_renewal_counter() -> non_neg_integer().
get_renewal_counter() ->
    comm:send_local(get_mock_pid(), {get_renewal_counter, self()}),
    receive
        {get_renewal_counter_response, Counter} ->
             Counter
    end.

-spec get_lease_list() -> lease_list:lease_list().
get_lease_list() ->
    comm:send_local(get_mock_pid(), {get, lease_list, self()}),
    receive
        {get_response, List} ->
             List
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(comm:message(), dht_node_state:state()) ->
                dht_node_state:state() | kill.
% get renewal counter
on({get_renewal_counter, Pid}, State) ->
    comm:send_local(Pid,
                    {get_renewal_counter_response,
                     erlang:get(renewal_counter)}),
    State;

% get from dht_node_state
on({get, Key, Pid}, State) ->
    comm:send_local(Pid,
                    {get_response,
                     dht_node_state:get(State, Key)}),
    State;

% intercept l_on_cseq messages
on(Msg, State) when element(1, Msg) =:= l_on_cseq ->
    Intercept = erlang:get(message_filter),
    if
        Intercept ->
            intercept_message(Msg, State);
        true ->
            case Msg of
                % intercept renew
                {l_on_cseq, renew, _OldLease, _Mode} ->
                    increment_renewal_counter(),
                    l_on_cseq:on(Msg, State);
                _ ->
                    l_on_cseq:on(Msg, State)
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Init
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init({}) -> dht_node_state:state().
init({}) ->
    DHTNodeGrp = pid_groups:group_with(dht_node),
    pid_groups:join_as(DHTNodeGrp, ?MODULE),
    ct:log("mock_l_on_cseq running on ~w~n", [self()]),
    erlang:put(message_filter, fun(_Msg) -> false end),
    erlang:put(renewal_counter, 0),
    dht_node_state:new(rt_external_rt, rm_loop_state, dht_db).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, {}, [{wait_for_init}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_mock_pid() -> pid() | failed.
get_mock_pid() ->
    pid_groups:find_a(?MODULE).

-spec increment_renewal_counter() -> ok.
increment_renewal_counter() ->
    erlang:put(erlang:get(renewal_counter) + 1).

-spec intercept_message(comm:message(), dht_node_state:state()) ->
                dht_node_state:state().
intercept_message(Msg, State) ->
    Owner = erlang:get(owner),
    comm:send_local(Owner, {intercepted_message, Msg}),
    State.