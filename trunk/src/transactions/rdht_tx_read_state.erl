%% @copyright 2009, 2010, 2012 onScale solutions GmbH

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
-module(rdht_tx_read_state).
-author('schintke@onscale.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-export([new/1,new_val/3,new_client/3]).
-export([get_id/1,
         set_client/2, get_client/1,
         set_key/2, get_key/1,
         inc_numok/1, get_numok/1,
         get_numfailed/1,
         get_result/1, set_result/2,
         get_decided/1, set_decided/2,
         get_numreplied/1,
         is_client_informed/1,
         set_client_informed/1,
         add_reply/5,
         update_decided/3,
         is_newly_decided/1]).

-include("scalaris.hrl").

-ifdef(with_export_type_support).
-export_type([read_state/0]).
-endif.

-type read_state() ::
                  { rdht_tx:req_id(),       % Id,
                    pid() | unknown, % ClientPid/unknown,
                    ?RT:key() | unknown,    % Key,
                    non_neg_integer(),      % NumOk,
                    non_neg_integer(),      % NumFail,
                    {?DB:value() | 0, integer()},     % Result = {Val, Vers},
                    tx_tlog:tx_status() | false, % is_decided
                    boolean()               % is_client_informed
                  }.

-spec new(rdht_tx:req_id()) -> read_state().
new(Id) ->
    {Id, unknown, unknown, 0, 0, {0, -1}, false, false}.
-spec new_val(rdht_tx:req_id(), ?DB:value(), integer()) -> read_state().
new_val(Id, Val, Vers) ->
    {Id, unknown, unknown, 0, 0, {Val, Vers}, false, false}.
-spec new_client(rdht_tx:req_id(), pid(), ?RT:key()) -> read_state().
new_client(Id, Pid, Key) ->
    {Id, Pid, Key, 0, 0, {0, -1}, false, false}.

-spec get_id(read_state()) -> rdht_tx:req_id().
get_id(State) ->             element(1, State).
-spec set_client(read_state(), pid()) -> read_state().
set_client(State, Pid) ->    setelement(2, State, Pid).
-spec get_client(read_state()) -> pid() | unknown.
get_client(State) ->         element(2, State).
-spec set_key(read_state(), ?RT:key()) -> read_state().
set_key(State, Key) ->       setelement(3, State, Key).
-spec get_key(read_state()) -> ?RT:key().
get_key(State) ->            element(3, State).
-spec inc_numok(read_state()) -> read_state().
inc_numok(State) ->          setelement(4, State, element(4, State) + 1).
-spec get_numok(read_state()) -> non_neg_integer().
get_numok(State) ->          element(4, State).
-spec inc_numfailed(read_state()) -> read_state().
inc_numfailed(State) ->      setelement(5, State, element(5, State) + 1).
-spec get_numfailed(read_state()) -> non_neg_integer().
get_numfailed(State) ->      element(5, State).
-spec get_result(read_state()) -> {any(), integer()}.
get_result(State) ->         element(6, State).
-spec set_result(read_state(), {any(), integer()}) -> read_state().
set_result(State, Val) ->    setelement(6, State, Val).
-spec get_decided(read_state()) -> tx_tlog:tx_status().
get_decided(State) ->        element(7, State).
-spec set_decided(read_state(), tx_tlog:tx_status()) -> read_state().
set_decided(State, Val) ->   setelement(7, State, Val).
-spec is_client_informed(read_state()) -> boolean().
is_client_informed(State) -> element(8, State).
-spec set_client_informed(read_state()) -> read_state().
set_client_informed(State) -> setelement(8, State, true).

-spec get_numreplied(read_state()) -> non_neg_integer().
get_numreplied(State) ->
    get_numok(State) + get_numfailed(State).

-spec add_reply(read_state(), ?DB:value(), integer(),
                non_neg_integer(), non_neg_integer()) -> read_state().
add_reply(State, Val, Vers, MajOk, MajDeny) ->
    ?TRACE("rdht_tx_read_state:add_reply state val vers majok majdeny ~p ~p ~p ~p ~p~n", [State, Val, Vers, MajOk, MajDeny]),
    {OldVal, OldVers} = get_result(State),
    NewResult = case Vers > OldVers of
                    true -> {Val, Vers};
                    false -> {OldVal, OldVers}
                end,
    TmpState = set_result(State, NewResult),
    NewState = case Vers >= 0 of
                   true -> inc_numok(TmpState);
                   false -> inc_numfailed(TmpState)
               end,
    update_decided(NewState, MajOk, MajDeny).

-spec update_decided(read_state(), non_neg_integer(),
                     non_neg_integer()) -> read_state().
update_decided(State, MajOk, MajDeny) ->
    ?TRACE("rdht_tx_read_state:update_decided state maj ~p ~p ~p~n",
           [State, MajOk, MajDeny]),
    OK = get_numok(State) >= MajOk,
    Abort = get_numfailed(State) >= MajDeny,
    {_, Vers} = get_result(State),
    case Vers =/= -1 of
        true ->
            if OK andalso (not Abort) ->
                    set_decided(State, value);
               (not OK) andalso Abort ->
                    set_decided(State, {fail, not_found});
               true -> State
            end;
        false ->
            case get_numreplied(State) of
                4 -> set_decided(State, {fail, not_found}); % all replied with -1
                _ -> State
            end
    end.

-spec is_newly_decided(read_state()) -> boolean().
is_newly_decided(State) ->
    ?TRACE("rdht_tx_read_state:is_newly_decided State ~p ~n", [State]),
    (false =/= get_decided(State))
        andalso (not is_client_informed(State)).
