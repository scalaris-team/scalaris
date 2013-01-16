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
                   state_is_newly_decided/1
                  ]}).

-type result() :: {?ok | fail, ?DB:value() | 0 | atom(), -1 | ?DB:version()}. % {?ok|fail, Val|FailReason, Vers}
-type read_state() ::
                  { ID               :: rdht_tx:req_id(),
                    ClientPid        :: pid() | unknown,
                    Key              :: ?RT:key() | unknown,
                    NumOk            :: non_neg_integer(),
                    NumFail          :: non_neg_integer(),
                    Result           :: result(),
                    IsDecided        :: tx_tlog:tx_status() | false,
                    IsClientInformed :: boolean(),
                    Op               :: ?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()}
                  }.

-spec state_new(rdht_tx:req_id()) -> read_state().
state_new(Id) ->
    {Id, unknown, unknown, 0, 0, {?ok, 0, -1}, false, false, ?read}.

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
-spec state_get_result(read_state()) -> result().
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
-spec state_get_op(read_state()) -> ?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()}.
state_get_op(State) ->          element(9, State).
-spec state_set_op(read_state(), ?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()}) -> read_state().
state_set_op(State, Op) ->     setelement(9, State, Op).

-spec state_get_numreplied(read_state()) -> non_neg_integer().
state_get_numreplied(State) ->
    state_get_numok(State) + state_get_numfailed(State).

-spec state_add_reply(read_state(),
                Result::{?ok, ?DB:value(), ?DB:version()} | {?ok, empty_val, -1} | {fail, atom(), ?DB:version()},
                non_neg_integer(), non_neg_integer()) -> read_state().
state_add_reply(State, Result, MajOk, MajDeny) ->
    ?TRACE("state_add_reply state res majok majdeny ~p ~p ~p ~p ~p~n", [State, Result, MajOk, MajDeny]),
    Vers = element(3, Result),
    OldVers = element(3, state_get_result(State)),
    TmpState = if Vers > OldVers -> state_set_result(State, Result);
                  true           -> State
               end,
    NewState = if Vers >= 0 -> state_inc_numok(TmpState);
                  true      -> state_inc_numfailed(TmpState)
               end,
    state_update_decided(NewState, MajOk, MajDeny).

-spec state_update_decided(read_state(), non_neg_integer(),
                     non_neg_integer()) -> read_state().
state_update_decided(State, MajOk, MajDeny) ->
    ?TRACE("state_update_decided state maj ~p ~p ~p~n",
           [State, MajOk, MajDeny]),
    {Ok_Fail, _Val, Vers} = state_get_result(State),
    if Vers =/= -1 ->
           OK = state_get_numok(State) >= MajOk,
           if OK andalso Ok_Fail =:= ?ok -> %% OK andalso (not Abort) ->
                  state_set_decided(State, ?value);
              OK -> state_set_decided(State, {fail, abort});
              true ->
                  Abort = state_get_numfailed(State) >= MajDeny,
                  if Abort -> %% (not OK) andalso Abort
                         state_set_decided(State, {fail, not_found});
                     true -> State
                  end
           end;
       true ->
           case state_get_numreplied(State) of
               4 -> state_set_decided(State, {fail, not_found}); % all replied with -1
               _ -> State
           end
    end.

-spec state_is_newly_decided(read_state()) -> boolean().
state_is_newly_decided(State) ->
    ?TRACE("state_is_newly_decided State ~p ~n", [State]),
    (false =/= state_get_decided(State))
        andalso (not state_is_client_informed(State)).
