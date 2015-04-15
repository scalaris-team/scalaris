% @copyright 2009-2012 Zuse Institute Berlin,
%                      onScale solutions GmbH

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

%% @author Florian Schintke <schintke@onscale.de>
%% @doc Part of replicated DHT implementation.
%%      The abstract datatype for the state for processing read operations.
%% @version $Id$

-compile({inline, [state_get_id/1,
                   state_get_client/1, state_set_client/2,
                   state_get_key/1, state_set_key/2,
                   state_get_numok/1, state_inc_numok/1,
                   state_get_numfailed/1, state_inc_numfailed/1,
                   state_get_result/1, state_set_result/2,
                   state_get_decided/1, state_set_decided/2,
                   state_is_client_informed/1, state_set_client_informed/1,
                   state_get_numreplied/1,
                   state_get_snapshot_number/1, state_set_snapshot_number/2
                  ]}).

% {?ok|fail, Val|FailReason, Vers}
-type result() :: {?ok | ?fail,
                   rdht_tx:encoded_value() | 0 | ?not_found | ?empty_list | ?not_a_list,
                   -1 | client_version()}.
-type read_state() ::
                  { ID               :: rdht_tx:req_id(),
                    ClientPid        :: pid() | unknown,
                    Key              :: ?RT:key() | unknown,
                    NumOk            :: non_neg_integer(),
                    NumFail          :: non_neg_integer(),
                    Result           :: result() | {?fail, 0, -2},
                    IsDecided        :: tx_tlog:tx_status() | false,
                    IsClientInformed :: boolean(),
                    SnapNumber       :: tx_tlog:snap_number(),
                    Op               :: ?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()}
                  }.
-type read_state_decided() ::
                  { ID               :: rdht_tx:req_id(),
                    ClientPid        :: pid() | unknown,
                    Key              :: ?RT:key() | unknown,
                    NumOk            :: non_neg_integer(),
                    NumFail          :: non_neg_integer(),
                    Result           :: result(),
                    IsDecided        :: tx_tlog:tx_status(),
                    IsClientInformed :: boolean(),
                    SnapNumber       :: tx_tlog:snap_number(),
                    Op               :: ?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()}
                  }.

-spec state_new(rdht_tx:req_id()) -> read_state().
state_new(Id) ->
    {Id, unknown, unknown, 0, 0, {?fail, 0, -2}, false, false, 0, ?read}.

-spec state_get_id(read_state()) -> rdht_tx:req_id().
state_get_id(State) ->              element(1, State).
-spec state_get_client(read_state()) -> pid() | unknown.
state_get_client(State) ->          element(2, State).
-spec state_set_client(read_state(), pid()) -> read_state().
state_set_client(State, Pid) ->     setelement(2, State, Pid).
-spec state_get_key(read_state()) -> ?RT:key() | unknown.
state_get_key(State) ->             element(3, State).
-spec state_set_key(read_state(), ?RT:key()) -> read_state().
state_set_key(State, Key) ->        setelement(3, State, Key).

-spec state_get_numok(read_state()) -> non_neg_integer().
state_get_numok(State) ->           element(4, State).
-spec state_inc_numok(read_state()) -> read_state().
state_inc_numok(State) ->           setelement(4, State, element(4, State) + 1).
-spec state_get_numfailed(read_state()) -> non_neg_integer().
state_get_numfailed(State) ->       element(5, State).
-spec state_inc_numfailed(read_state()) -> read_state().
state_inc_numfailed(State) ->       setelement(5, State, element(5, State) + 1).
-spec state_get_result(read_state()) -> result() | {?fail, 0, -2}.
state_get_result(State) ->          element(6, State).
-spec state_set_result(read_state(), result()) -> read_state().
state_set_result(State, Val) ->     setelement(6, State, Val).
-spec state_get_decided(read_state()) -> tx_tlog:tx_status() | false.
state_get_decided(State) ->         element(7, State).
-spec state_set_decided(read_state(), tx_tlog:tx_status()) -> read_state().
state_set_decided(State, Val) ->    setelement(7, State, Val).
-spec state_is_client_informed(read_state()) -> boolean().
state_is_client_informed(State) ->  element(8, State).
-spec state_set_client_informed(read_state()) -> read_state().
state_set_client_informed(State) -> setelement(8, State, true).
-spec state_get_snapshot_number(read_state()) -> tx_tlog:snap_number().
state_get_snapshot_number(State) -> element(9, State).
-spec state_set_snapshot_number(read_state(), tx_tlog:snap_number()) -> read_state().
state_set_snapshot_number(State, Val) -> setelement(9, State, Val).
-spec state_get_op(read_state()) -> ?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()}.
state_get_op(State) ->          element(10, State).
-spec state_set_op(read_state(), ?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()}) -> read_state().
state_set_op(State, Op) ->     setelement(10, State, Op).

-spec state_get_numreplied(read_state()) -> non_neg_integer().
state_get_numreplied(State) ->
    state_get_numok(State) + state_get_numfailed(State).

-spec state_add_reply(read_state(),
                      Result::{?ok, rdht_tx:encoded_value(), client_version()} | {?ok, empty_val, -1} |
                          {?fail, ?not_found | ?empty_list | ?not_a_list,
                           client_version()}, tx_tlog:snap_number())
        -> read_state().
state_add_reply(State, Result, SnapNumber) ->
    ?TRACE("state_add_reply state res majok majdeny ~p ~p ~n", [State, Result]),
    Vers = element(3, Result),
    OldVers = element(3, state_get_result(State)),
    TmpState = if Vers > OldVers -> state_set_result(State, Result);
                  true           -> State
               end,
    NewState = if Vers >= 0 -> state_inc_numok(TmpState);
       true      -> state_inc_numfailed(TmpState)
    end,
    state_set_snapshot_number(NewState, SnapNumber).
