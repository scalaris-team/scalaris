%  @copyright 2012 Zuse Institute Berlin

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

%% @author Jan Fajerski <fajerski@zib.de>
%% @doc state for one map reduce job including the mr database
%% @version $Id$
-module(mr_state).
-author('fajerski@zib.de').
-vsn('$Id$').

-define(TRACE(X, Y), ok).
%% -define(TRACE(X, Y), io:format(X, Y)).

%% -export([new/6
%%         , get/2
%%         , get_next_phase/1
%%         , add_data_to_next_phase/2]).
-compile[export_all].

-include("scalaris.hrl").
%% for ?required macro
-include("record_helpers.hrl").

-type(fun_term() :: {erlanon, binary()} | {jsanon, binary()}).

-type(mr_phase() :: {map | reduce, fun_term(), Keep::boolean(), Input::[any()]}).

-type(jobid() :: nonempty_string()).

-record(state, {jobid       = ?required(state, jobid) :: jobid()
                , client    = null :: comm:mypid() | null
                , master    = null :: comm:mypid() | null
                , phases    = ?required(state, phases) :: [mr_phase()]
                , my_range  = ?required(state, myrange) :: intervals:interval()
                , current   = 1 :: pos_integer()
                , acked     = {null, []} :: {null | reference(), intervals:interval()}
               }).

-type(state() :: #state{}).

-spec get(state(), my_range)        -> intervals:interval();
         (state(), client)          -> comm:mypid().
get(#state{client = Client
           , master = Master
           , jobid = JobId
           , my_range = Range
           , phases = Phases
           , current = Cur
          }, Key) ->
    case Key of
        client -> Client;
        master -> Master;
        my_range -> Range;
        phases -> Phases;
        current -> Cur;
        jobid  -> JobId
    end.

-spec new(jobid(), comm:mypid(), comm:mypid(), [tuple()], [mr_phase],
          intervals:interval()) ->
    state().
new(JobId, Client, Master, InitalData, Phases, Range) ->
    ?TRACE("mr_state: ~p~nnew state from: ~p~n", [comm:this(), {JobId, Client,
                                                                Master,
                                                                InitalData,
                                                                Phases, Range}]),
    PhasesWithData = lists:zipwith(
            fun({MoR, Fun, Keep}, {Round, Data}) -> 
                    {Round, MoR, Fun, Keep, Data}
            end, Phases, [{1, InitalData} | [{I, []} || I <- lists:seq(2,
                                                             length(Phases))]]),
    NewState = #state{jobid = JobId
           , client = Client
           , master = Master
           , phases = PhasesWithData
           , my_range = Range
          },
    NewState.

-spec next_phase(state()) -> state().
next_phase(State = #state{current = Cur}) ->
    State#state{current = Cur + 1}.

-spec is_last_phase(state()) -> boolean().
is_last_phase(#state{current = Cur, phases = Phases}) ->
    Cur =:= length(Phases).

-spec get_phase(state()) -> mr_phase().
get_phase(#state{phases = Phases, current = Cur}) ->
    lists:keyfind(Cur, 1, Phases).

is_acked_complete(#state{acked = {_Ref, Interval}}) ->
    intervals:is_all(Interval).

%% TODO find a robust way to check for the same ref but only when not resetting
set_acked(State = #state{acked = {_OldRef, _Interval}}, {NewRef, []}) ->
    State#state{acked = {NewRef, []}};
set_acked(State = #state{acked = {Ref, Interval}}, {Ref, NewInterval}) ->
    State#state{acked = {Ref, intervals:union(Interval, NewInterval)}}.

-spec add_data_to_next_phase(state(), [any()]) -> state().
add_data_to_next_phase(State = #state{phases = Phases, current = Cur}, NewData) ->
    {Round, MoR, Fun, Keep, Data} = lists:keyfind(Cur + 1, 1, Phases),
    State#state{phases = lists:keyreplace(Cur + 1, 1, Phases, {Round, MoR, Fun,
                                                            Keep, NewData ++
                                                            Data})}.


