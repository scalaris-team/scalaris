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

%
-export([get_renewal_counter/0]).

-record(mock_state, {
          renewal_enabled      = ?required(mock_state, renewal_enabled     ) :: boolean(),
          renewal_counter      = ?required(mock_state, renewal_counter     ) :: non_neg_integer()
         }).

-type mock_state_t() :: #mock_state{}.

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

-spec create_lease(?RT:key(), ?RT:key()) -> {l_on_cseq:lease_t(), l_on_cseq:lease_t()}.
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(comm:message(), {dht_node_state:state(), mock_state_t()}) ->
                {dht_node_state:state(), mock_state_t()} | kill.
% get renewal counter
on({get_renewal_counter, Pid},
   {State, MockState}) ->
    comm:send_local(Pid,
                    {get_renewal_counter_response,
                     get_renewal_counter(MockState)}),
    {State, MockState};

% intercept renews
on({l_on_cseq, renew, _OldLease, _Mode} = Msg,
   {State, MockState}) ->
    {l_on_cseq:on(Msg, State), increment_renewal_counter(MockState)};

% Lease management messages (see l_on_cseq.erl)
on(Msg, {State, MockState}) when l_on_cseq =:= element(1, Msg) ->
    {l_on_cseq:on(Msg, State), MockState}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Init
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init(any()) -> {dht_node_state:state(), mock_state_t()}.
init(_) ->
    DHTNodeGrp = pid_groups:group_with(dht_node),
    pid_groups:join_as(DHTNodeGrp, ?MODULE),
    ct:log("mock_l_on_cseq running on ~w~n", [self()]),
    {dht_node_state:new(rt_external_rt, rm_loop_state, dht_db), new_mock_state()}.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, {}, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% mock_state
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_mock_pid() -> pid() | failed.
get_mock_pid() ->
    pid_groups:find_a(?MODULE).

-spec new_mock_state() -> mock_state_t().
new_mock_state() ->
    #mock_state{renewal_enabled = true,
                renewal_counter = 0}.

-spec increment_renewal_counter(mock_state_t()) -> mock_state_t().
increment_renewal_counter(#mock_state{renewal_counter=Counter} = MockState) ->
    MockState#mock_state{renewal_counter = Counter+1}.

-spec get_renewal_counter(mock_state_t()) -> non_neg_integer().
get_renewal_counter(#mock_state{renewal_counter=Counter}) ->
    Counter.
