%% @copyright 2007-2013 Zuse Institute Berlin

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

-type(message() :: {work, Source::comm:mypid(), wpool:job()}).

-type(state() :: {}).

-spec init([]) -> state().
init([]) ->
    {}.

-spec start_link(tuple()) -> {ok, pid()}.
start_link(Options) ->
    ?TRACE("wpool_worker: starting...~n", []),
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [], [Options]).

-spec on(message(), state()) -> state().
on({work, Source, Workload}, State) ->
    work(Source, Workload),
    State;

on(Msg, State) ->
    ?TRACE("~200p~nwpool_worker: unknown message~n", [Msg]),
    State.


%% @doc do the actual work.
%%      executes Job and returns results to the local wpool. wpool associates
%%      the worker pid to the jobs client and knows where the results go.
-spec work(comm:mypid (), wpool:job()) -> ok.
work(Source, {_Round, map, {erlanon, Fun}, Data, ResTable}) ->
    ?TRACE("worker: should apply ~p to ~p~n", [FunBin, Data]),
    Results =
    ets:foldl(fun({_HK, K, V}, Acc) ->
                      Res = apply_erl(Fun, {K, V}),
                      mr_state:accumulate_data(Res, Acc)
              end,
              ResTable, Data),
    return(Source, Results);
work(Source, {_Round, reduce, {erlanon, Fun}, Data, Acc}) ->
    Res = apply_erl(Fun,
                    ets:foldl(fun({_HK, K, V}, AccFold) -> [{K, V} | AccFold] end,
                              [],
                              Data)),
    %% TODO insert can handle lists
    Results = lists:foldl(fun({K, V}, ETSAcc) ->
                        ets:insert(ETSAcc, {?RT:hash_key(K), K, V}),
                        ETSAcc
                end,
                Acc,
                Res),
    return(Source, Results);

work(Source, {_Round, map, {jsanon, FunBin}, Data, ResTable}) ->
    %% ?TRACE("worker: should apply ~p to ~p~n", [FunBin, Data]),
    {ok, VM} = js_driver:new(),
    Results =
    ets:foldl(fun(E, Acc) ->
                      Res = apply_js(FunBin, [E], VM),
                      mr_state:accumulate_data(Res, Acc)
              end,
              ResTable, Data),
    return(Source, Results);
work(Source, {_Round, reduce, {jsanon, FunBin}, Data, Acc}) ->
    {ok, VM} = js_driver:new(),
    Res = apply_js(FunBin, [ets:tab2list(Data)], VM),
    %% TODO insert can handle lists
    Results = lists:foldl(fun({K, V}, ETSAcc) ->
                        ets:insert(ETSAcc, {K, V}),
                        ETSAcc
                end,
                Acc,
                Res),
    return(Source, Results).

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
    EncodedArgs = encode_args(Args, []),
    iolist_to_binary([FunBin, "(", EncodedArgs, ")"]).

%% @doc build argument list for JS function.
%%      takes a list of terms and returns an iolist of the encoded (mochijson2)
%%      terms with commas in between.
-spec encode_args([term()], [term()]) -> term().
encode_args([], Acc) ->
    lists:reverse(Acc);
encode_args([H | []], Acc) ->
    encode_args([], [js_mochijson2:encode(encode(H)) | Acc]);
encode_args([H | T], Acc) ->
    encode_args(T, [[js_mochijson2:encode(encode(H)), ","] | Acc]).

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
-spec return(comm:mypid(), any()) -> ok.
return(Source, Data) ->
    comm:send_local(Source, {data, self(), Data}).

