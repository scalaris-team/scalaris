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
%% @doc state for one map reduce node
%% @version $Id$
-module(mr_state).
-author('fajerski@zib.de').
-vsn('$Id$').

-define(TRACE(X, Y), ok).
%% -define(TRACE(X, Y), io:format(X, Y)).
-define(TRACE_SLIDE(X, Y), ok).
%% -define(TRACE_SLIDE(X, Y), io:format(X, Y)).

-define(DEF_OPTIONS, []).

-export([new/6
        , get/2
        , get_phase/2
        , is_acked_complete/1
        , set_acked/2
        , reset_acked/1
        , is_last_phase/2
        , add_data_to_phase/4
        , interval_processing/3
        , interval_processed/3
        , interval_empty/3
        , accumulate_data/2
        , clean_up/1
        , split_slide_state/2
        , init_slide_phase/1
        , get_slide_delta/2
        , add_slide_delta/2
        , tester_is_valid_funterm/1
        , tester_create_valid_funterm/2]).

-include("scalaris.hrl").
-include("client_types.hrl").
%% for ?required macro
-include("record_helpers.hrl").

-ifdef(with_export_type_support).
-export_type([data/0, jobid/0, state/0, fun_term/0, data_list/0]).
-endif.

-type(erl_fun() :: {map, erlanon, fun((Arg::{client_key(), term()}) ->
                                          Res::[{client_key(), term()}])}
                 | {reduce, erlanon, fun((Arg::[{client_key(), term()}]) ->
                                            Res::[{client_key(), term()}])}).

-type(js_fun() :: {map | reduce, jsanon, binary()}).

-type(fun_term() :: erl_fun() | js_fun()).

-type(data_list() :: [{?RT:key(), string(), term()}]).
%% data in ets table has the same format
-type(data_ets() :: db_ets:db()).

-type(data() :: data_list() | data_ets()).

-type(delta_phase() :: {PhaseNr::pos_integer(), fun_term(),
                        Input::data_list(), ToWorkOn::intervals:interval(),
                        WorkingOn::intervals:interval()}).

-type(ets_phase() :: {PhaseNr::pos_integer(), fun_term(),
                      Input::data_ets(), ToWorkOn::intervals:interval(),
                      WorkingOn::intervals:interval()}).

-type(phase() :: ets_phase() | delta_phase()).

%% only allow strings that can be converted to atoms (used for ets names)
-type(jobid() :: [0..255,...]).

-record(state, {jobid       = ?required(state, jobid) :: jobid()
                , client    = false :: comm:mypid() | false
                , master_id = ?required(state, master_id) :: ?RT:key()
                , phases    = ?required(state, phases) :: [ets_phase(),...]
                , options   = ?required(state, options) :: [mr:option()]
                , acked     = intervals:empty() :: intervals:interval()
               }).

-type(state() :: #state{}).

-spec get(state(), client)          -> comm:mypid() | false;
         (state(), jobid)           -> jobid();
         (state(), phases)          -> [phase()];
         (state(), master_id)       -> ?RT:key();
         (state(), options)         -> [mr:option()].
get(#state{client        = Client
           , master_id   = Master
           , jobid       = JobId
           , phases      = Phases
           , options     = Options
          }, Key) ->
    case Key of
        client      -> Client;
        master_id   -> Master;
        phases      -> Phases;
        options     -> Options;
        jobid       -> JobId
    end.

-spec new(jobid(), comm:mypid(), ?RT:key(), data_list(),
          mr:job_description(), intervals:interval()) ->
    state().
new(JobId, Client, Master, InitalData, {Phases, Options}, Interval) ->
    ?TRACE("mr_state: ~p~nnew state from: ~p~n", [comm:this(), {JobId, Client,
                                                                Master,
                                                                InitalData,
                                                                {Phases,
                                                                 Options}}]),
    InitalETS = db_ets:new(
                    lists:append(["mr_", JobId, "_1"]), [ordered_set]),
    DB = db_ets:put(InitalETS, InitalData),
    ExtraData = [{1, DB, Interval, intervals:empty()} |
                 [{I, db_ets:new(
                        lists:flatten(io_lib:format("mr_~s_~p", [JobId, I]))
                        , [ordered_set]), intervals:empty(), intervals:empty()}
                  || I <- lists:seq(2, length(Phases))]],
    PhasesWithData = lists:zipwith(
            fun(Fun, {Round, Data, Open, Working}) ->
                    {Round, Fun, Data, Open, Working}
            end, Phases, ExtraData),
    JobOptions = merge_with_default_options(Options, ?DEF_OPTIONS),
    NewState = #state{
                  jobid      = JobId
                  , client   = Client
                  , master_id   = Master
                  , phases   = PhasesWithData
                  , options  = JobOptions
          },
    NewState.

-spec is_last_phase(state(), pos_integer()) -> boolean().
is_last_phase(#state{phases = Phases}, Round) ->
    Round =:= length(Phases).

-spec get_phase(state(), pos_integer()) -> phase() | false.
get_phase(#state{phases = Phases}, Round) ->
    lists:keyfind(Round, 1, Phases).

-spec is_acked_complete(state()) -> boolean().
is_acked_complete(#state{acked = Interval}) ->
    intervals:is_all(Interval).

-spec reset_acked(state()) -> state().
reset_acked(State) ->
    State#state{acked = intervals:empty()}.

-spec set_acked(state(), intervals:interval()) -> state().
set_acked(State = #state{acked = Interval}, NewInterval) ->
    State#state{acked = intervals:union(Interval, NewInterval)}.

-spec interval_processing(state(), intervals:interval(), pos_integer()) ->
    state().
interval_processing(State = #state{phases = Phases}, Interval, Round) ->
    case lists:keyfind(Round, 1, Phases) of
        {Round, Fun, ETS, Open, Working} ->
            NewPhases = lists:keyreplace(Round, 1, Phases,
                                         {Round, Fun, ETS,
                                          intervals:minus(Open, Interval),
                                          intervals:union(Working, Interval)}),
            ?TRACE("start working on ~p new open is ~p~n", [Interval,
                                                            intervals:minus(Open,
                                                                            Interval)]),
            State#state{phases = NewPhases};
        false ->
            State
    end.

-spec interval_processed(state(), intervals:interval(), pos_integer()) ->
    state().
interval_processed(State = #state{phases = Phases}, Interval, Round) ->
    case lists:keyfind(Round, 1, Phases) of
        {Round, Fun, ETS, Open, Working} ->
            NewPhases = lists:keyreplace(Round, 1, Phases,
                                         {Round, Fun, ETS,
                                          Open,
                                          intervals:minus(Working, Interval)}),
            State#state{phases = NewPhases};
        false ->
            State
    end.

-spec interval_empty(state(), intervals:interval(), pos_integer()) ->
    state().
interval_empty(State = #state{phases = Phases}, Interval, Round) ->
    case lists:keyfind(Round, 1, Phases) of
        {Round, Fun, ETS, Open, Working} ->
            NewPhases = lists:keyreplace(Round, 1, Phases,
                                         {Round, Fun, ETS,
                                          intervals:minus(Open, Interval),
                                          Working}),
            State#state{phases = NewPhases};
        false ->
            State
    end.

%% NEXT register new as value creator
-spec add_data_to_phase(state(), data_list(), intervals:interval(),
                             pos_integer()) -> state().
add_data_to_phase(State = #state{phases = Phases}, NewData,
                      Interval, Round) ->
    case lists:keyfind(Round, 1, Phases) of
        {Round, Fun, ETS, Open, Working} ->
            %% side effect is used here...only works with ets
            _ = accumulate_data(NewData, ETS),
            NextPhase = {Round, Fun, ETS, intervals:union(Open,
                                                               Interval),
                         Working},
            State#state{phases = lists:keyreplace(Round, 1, Phases, NextPhase)};
        false ->
            %% someone tries to add data to nonexisting phase...do nothing
            State
    end.

-spec accumulate_data([{client_key(), term()}], data()) -> data().
accumulate_data(Data, List) when is_list(List) ->
    lists:foldl(fun({K, V}, Acc) ->
                        HK = ?RT:hash_key(K),
                        acc_add_element(Acc, {HK, K, V});
                   ({HK, K, V}, Acc) ->
                        acc_add_element(Acc, {HK, K, V})
                end,
                List,
                Data);
accumulate_data(Data, ETS) ->
    ?TRACE("accumulating ~p~n", [Data]),
    lists:foldl(fun({K, V}, ETSAcc) ->
                        HK = ?RT:hash_key(K),
                        acc_add_element(ETSAcc, {HK, K, V});
                   ({HK, K, V}, ETSAcc) ->
                        acc_add_element(ETSAcc, {HK, K, V})
                end,
                ETS,
                Data).

-spec acc_add_element(data(), {?RT:key(), client_key(), term()}) ->
    data().
acc_add_element(List, {HK, K, V} = T) when is_list(List) ->
    case lists:keyfind(HK, 1, List) of
        false ->
            case is_list(V) of
                true ->
                    [T | List];
                _ ->
                    [{HK, K, [V]} | List]
            end;
        {HK, K, ExV} ->
            case is_list(V) of
                true ->
                    [{HK, K, V ++ ExV} | List];
                _ ->
                    [{HK, K, [V | ExV]} | List]
            end
    end;
acc_add_element(ETS, {HK, K, V}) ->
    case db_ets:get(ETS, HK) of
        {} ->
            case is_list(V) of
                true ->
                    db_ets:put(ETS, {HK, K, V});
                _ ->
                    db_ets:put(ETS, {HK, K, [V]})
            end;
        {HK, K, ExV} ->
            case is_list(V) of
                true ->
                    db_ets:put(ETS, {HK, K, V ++ ExV});
                _ ->
                    db_ets:put(ETS, {HK, K, [V | ExV]})
            end
    end.

-spec merge_with_default_options(UserOptions::[mr:option()],
                                 DefaultOptions::[mr:option()]) ->
    JobOptions::[mr:option()].
merge_with_default_options(UserOptions, DefaultOptions) ->
    %% TODO merge by hand and skip everything that is not in DefaultOptions
    lists:keymerge(1,
                   lists:keysort(1, UserOptions),
                   lists:keysort(1, DefaultOptions)).

%% TODO fix types data as ets or list

-spec clean_up(state()) -> [true].
clean_up(#state{phases = Phases}) ->
    lists:map(fun({_R, _Fun, ETS, _Interval, _Working}) ->
                      db_ets:close(ETS)
              end, Phases).

-spec split_slide_state(state(), intervals:interval()) -> SlideState::state().
split_slide_state(#state{phases = Phases} = State, _Interval) ->
    SlidePhases =
    lists:foldl(
      fun({Nr, Fun, _ETS, _Open, _Working}, Slide) ->
              [{Nr, Fun, false, intervals:empty(), intervals:empty()} | Slide]
      end,
      [],
      Phases),
    State#state{phases = SlidePhases}.

-spec init_slide_phase(state()) -> state().
init_slide_phase(State = #state{phases = Phases, jobid = JobId}) ->
    PhasesETS = lists:foldl(
                  fun({Nr, Fun, false, Open, Working}, AccIn) ->
                          ETS = db_ets:new(
                                  lists:flatten(io_lib:format("mr_~s_~p",
                                                              [JobId, Nr]))
                                  , [ordered_set]),
                          [{Nr, Fun, ETS, Open, Working} | AccIn];
                     (Phase, AccIn) ->
                          [Phase | AccIn]
                  end, [], Phases),
    State#state{phases = PhasesETS}.

-spec get_slide_delta(state(), intervals:interval()) -> {state(), [delta_phase()]}.
get_slide_delta(State = #state{phases = Phases}, SlideInterval) ->
    {NewPhases, SlidePhases} =
    lists:foldl(
      fun({Nr, Fun, ETS, Open, Working}, {PhaseAcc, SlideAcc}) ->
              SlideData = lists:foldl(
                            fun(SimpleInterval, AccI) ->
                                    db_ets:foldl(ETS,
                                                 fun(K, Acc) ->
                                                         Entry = db_ets:get(ETS, K),
                                                         _NewDB = db_ets:delete(ETS, K),
                                                         [Entry | Acc]
                                                 end,
                                                 AccI,
                                                 SimpleInterval)
                            end, [],
                            intervals:get_simple_intervals(SlideInterval)),
              NewOpen = intervals:minus(Open, SlideInterval),
              SlideOpen = intervals:intersection(Open, SlideInterval),
              {[{Nr, Fun, ETS, NewOpen, Working} | PhaseAcc],
               [{Nr, Fun, SlideData, SlideOpen, intervals:empty()} | SlideAcc]}
      end,
      {[], []},
      Phases),
    {State#state{phases = NewPhases}, SlidePhases}.

-spec add_slide_delta(state(), [delta_phase()]) -> state().
add_slide_delta(State = #state{jobid = JobId,
                               phases = Phases}, DeltaPhases) ->
    MergedPhases = lists:map(
                  fun(DeltaPhase) ->
                          merge_phase_delta(lists:keyfind(element(1, DeltaPhase),
                                                          1,
                                                          Phases),
                                            DeltaPhase)
                          end, DeltaPhases),
    ?TRACE_SLIDE("mr_~p on ~p: received delta: ~p~n", [JobId, self(), DeltaPhases]),
    trigger_work(MergedPhases, JobId),
    State#state{phases = MergedPhases}.

-spec merge_phase_delta(ets_phase(), delta_phase()) -> ets_phase().
merge_phase_delta({Round, Fun, ETS, Open, Working},
                  {Round, Fun, Delta, DOpen, _DWorking}) ->
    %% side effect
    %% also should be db_ets, but ets can add lists
    ets:insert(ETS, Delta),
    {Round, Fun, ETS, intervals:union(Open, DOpen),
     Working}.

-spec trigger_work([ets_phase()], jobid()) -> ok.
trigger_work(Phases, JobId) ->
    SmallestOpenPhase = lists:foldl(
                         fun({Round, _, _, Open, _}, Acc) ->
                                 case not intervals:is_empty(Open)
                                      andalso Round < Acc of
                                     true ->
                                         Round;
                                     _ ->
                                         Acc
                                 end
                         end, length(Phases) + 1, Phases),
    case SmallestOpenPhase of
        N when N < length(Phases) ->
            comm:send_local(self(), {mr, next_phase, JobId, N, intervals:empty()});
        _N ->
            ok
    end.

-spec tester_is_valid_funterm(fun_term()) -> boolean().
tester_is_valid_funterm({map, erlanon, Fun}) when is_function(Fun, 1) -> true;
tester_is_valid_funterm({reduce, erlanon, Fun}) when is_function(Fun, 1) -> true;
tester_is_valid_funterm({map, jsanon, Fun}) when is_binary(Fun) -> true;
tester_is_valid_funterm({reduce, jsanon, Fun}) when is_binary(Fun) -> true;
tester_is_valid_funterm(_) -> false.

-spec tester_create_valid_funterm(erlanon | jsanon, map | reduce) -> fun_term().
tester_create_valid_funterm(erlanon, map) ->
    {map, erlanon, fun({"Foo", bar}) -> [{"Foo", bar}] end};
tester_create_valid_funterm(erlanon, reduce) ->
    {reduce, erlanon, fun([{"Foo", bar}]) -> [{"Foo", bar}] end};
tester_create_valid_funterm(jsanon, MoR) ->
    {MoR, jsanon, <<"some js function">>}.
