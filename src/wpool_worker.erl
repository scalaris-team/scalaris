%% @copyright 2007-2015 Zuse Institute Berlin

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
%% @doc worker pool worker implementation.
%%
%% @end
%% @version $Id$
-module(wpool_worker).
-author('fajerski@zib.de').
-vsn('$Id$ ').

-define(TRACE(X, Y), ok).
%% -define(TRACE(X, Y), io:format(X, Y)).

-behaviour(gen_component).

-export([
        start_link/1
        , on/2
        , init/1
        ]).

-include("scalaris.hrl").
-include("gen_component.hrl").

-type(message() :: {work, Source::comm:erl_local_pid(), wpool:job()}).

-type(state() :: {}).

-spec init([]) -> state().
init([]) ->
    {}.

-spec start_link(tuple()) -> {ok, pid()}.
start_link(Options) ->
    ?TRACE("~p wpool_worker: starting...~n", [self()]),
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [], [Options]).

-spec on(message(), state()) -> state().
on({work, Source, Workload}, State) ->
    work(Source, Workload),
    State;

on(_Msg, State) ->
    ?TRACE("~200p~nwpool_worker: unknown message~n", [Msg]),
    State.


%% @doc do the actual work.
%%      executes Job and returns results to the local wpool. wpool associates
%%      the worker pid to the jobs client and knows where the results go.
-spec work(comm:erl_local_pid(), wpool:job()) -> ok.
work(Source, {{map, erlanon, Fun}, Data, Interval}) ->
    ?TRACE("worker: should apply map ~p to ~p in ~p~n", [Fun, Data, Interval]),
    Results =
    lists:foldl(fun(SimpleInterval, Acc1) ->
                        db_ets:foldl(Data,
                                     fun(HK, Acc) ->
                                             {_HK, K, V} = db_ets:get(Data, HK),
                                             ?TRACE("mapping ~p~n", [{K, V}]),
                                             Res = apply_erl(Fun, {K, V}),
                                             mr_state:accumulate_data(Res, Acc)
                                     end,
                                     Acc1, SimpleInterval)
                end,
                [],
                intervals:get_simple_intervals(Interval)),
    return(Source, Results);
work(Source, {{reduce, erlanon, Fun}, Data, Interval}) ->
    ?TRACE("~p worker: should apply reduce ~p~n    in ~p~n",
           [self(), ets:tab2list(Data), Interval]),
    Args = lists:foldl(fun(SimpleInterval, Acc1) ->
                               db_ets:foldl(Data,
                                            fun(HK, AccFold) ->
                                                    {_HK, K, V} =
                                                    db_ets:get(Data, HK),
                                                    [{K, V} | AccFold]
                                            end,
                                         Acc1,
                                         SimpleInterval)
                       end,
                       [],
                       intervals:get_simple_intervals(Interval)),
    ?TRACE("~p reducing ~p~n", [self(), Args]),
    Res = apply_erl(Fun, Args),
    return(Source, [{?RT:hash_key(K), K, V} || {K, V} <- Res]);

work(Source, {{map, jsanon, Fun}, Data, Interval}) ->
    %% ?TRACE("worker: should apply ~p to ~p~n", [FunBin, Data]),
    {ok, VM} = js_driver:new(),
    Results =
    lists:foldl(fun(SimpleInterval, Acc1) ->
                        db_ets:foldl(Data,
                                     fun(HK, Acc) ->
                                             {_HK, K, V} = db_ets:get(Data, HK),
                                             Res = apply_js(Fun, [{K, V}], VM),
                                             mr_state:accumulate_data(Res, Acc)
                                     end,
                                     Acc1, SimpleInterval)
                end,
                [],
                intervals:get_simple_intervals(Interval)),
    return(Source, Results);
work(Source, {{reduce, jsanon, Fun}, Data, Interval}) ->
    {ok, VM} = js_driver:new(),
    Args = lists:foldl(fun(SimpleInterval, Acc1) ->
                               db_ets:foldl(Data,
                                            fun(HK, AccFold) ->
                                                    {_HK, K, V} =
                                                    db_ets:get(Data, HK),
                                                    [{K, V} | AccFold]
                                            end,
                                         Acc1,
                                         SimpleInterval)
                       end,
                       [],
                       intervals:get_simple_intervals(Interval)),
    Res = apply_js(Fun, [Args], VM),
    return(Source, [{?RT:hash_key(K), K, V} || {K, V} <- Res]).
%% TODO add generic work loads

%% @doc applies Fun with Args.
-spec apply_erl(Fun::fun((Arg::term()) -> Res::A), Args::term()) -> A.
apply_erl(Fun, Data) ->
    Fun(Data).

%% @doc applies Fun with Args withing JSVM.
-spec apply_js(Fun::binary(), Args::[term()], JSVM::port()) -> term().
apply_js(FunBin, Data, VM) ->
    AutoJS = define_auto_js(FunBin, Data),
    {ok, Result} = js:eval(VM, AutoJS),
    decode(Result).

%% @doc create a self calling JS function.
%%      takes a anonymous JS function (as a binary string) and a list of
%%      arguments and returns a self calling JS (`function(arg) {...}(args)')
%%      function as a binary string.
-spec define_auto_js(Fun::binary(), [term()]) -> binary().
define_auto_js(FunBin, Args) ->
    EncodedArgs = encode_args(Args),
    iolist_to_binary([FunBin, "(", EncodedArgs, ")"]).

%% @doc build argument list for JS function.
%%      takes a list of terms and returns an iolist of the encoded (mochijson2)
%%      terms with commas in between.
-spec encode_args([term()]) -> iolist().
encode_args([]) ->
    [];
encode_args([H]) ->
    js_mochijson2:encode(encode(H));
encode_args([H | T]) ->
    [js_mochijson2:encode(encode(H)), "," | encode_args(T)].

-spec encode(term()) -> term().
encode({K, V}) ->
    {struct, [{encode(list_to_binary(K)), encode(V)}]};
encode([{_K, _V} | _T] = KVList) ->
    %% {struct, [{encode(K), encode(V)} || {K, V} <- KVList]};
    %% [{struct, [{key, encode(K)}, {value, encode(V)}]} || {K, V} <- KVList];
    Fun = fun({K, V}, AccIn) ->
                  %% strings need to be encoded as binaries
                  %% keys are always strings; if value is a string it needs to
                  %% be passed as a binary
                  EnK = encode(list_to_binary(K)), EnV = encode(V),
                  [{EnK, EnV} | AccIn]
          end,
    Data = lists:foldl(Fun, [], KVList),
    {struct, Data};
%% this poses a problem with lists as values. if value is a list it gets encoded
%% to a binary and does not get encoded as an array.
%% encode(String) when is_list(String) ->
%%     list_to_binary(String);
encode(X) -> X.

-spec decode(term()) -> term().
decode({struct, [{K, V}]}) ->
    {decode(K), decode(V)};
decode({struct, [{_K, _V} | _T] = KVList}) ->
    [{decode(K), decode(V)} || {K, V} <- KVList];
decode([{struct, _KV} | _T] = List) ->
    [{decode(K), decode(V)} || {struct, [{K, V}]} <- List];
decode(BinString) when is_binary(BinString) ->
    binary_to_list(BinString);
decode(X) -> X.

%% send results back to wpool
-spec return(comm:erl_local_pid(), [term()]) -> ok.
return(Source, Data) ->
    comm:send_local(Source, {data, self(), Data}).

