% @copyright 2014 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc Methods for (interactive) debugging.
%% @version $Id$
-module(debug).
-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("client_types.hrl").

-export([get_round_trip/2]).
-export([
         dump/0, dump2/0, dump3/0, dumpX/1, dumpX/2,
         topDumpX/1, topDumpX/3,
         topDumpXEvery/3, topDumpXEvery/5, topDumpXEvery_helper/4
        ]).
-export([rr_count_old_replicas/2]).

%% @doc Simple round-trip benchmark to an arbitrary gen_component.
-spec get_round_trip(GPid::comm:mypid(), Iterations::pos_integer()) -> float().
get_round_trip(GPid, Iterations) ->
    Start = os:timestamp(),
    get_round_trip_helper(GPid, Iterations),
    End = os:timestamp(),
    timer:now_diff(End, Start) / Iterations.

%% @doc Helper for get_round_trip/2.
-spec get_round_trip_helper(GPid::comm:mypid(), Iterations::pos_integer()) -> ok.
get_round_trip_helper(_GPid, 0) ->
    ok;
get_round_trip_helper(GPid, Iterations) ->
    comm:send(GPid, {ping, comm:this()}),
    receive _Any -> ok end,
    get_round_trip_helper(GPid, Iterations - 1).

%% @doc Extracts a given ItemInfo from an ItemList that has been returned from
%%      e.g. erlang:process_info/2 for the dump* methods.
-spec dump_extract_from_list
        ([{Item::atom(), Info::term()}], ItemInfo::memory | message_queue_len | stack_size | heap_size) -> non_neg_integer();
        ([{Item::atom(), Info::term()}], ItemInfo::messages) -> [tuple()];
        ([{Item::atom(), Info::term()}], ItemInfo::current_function) -> Fun::mfa();
        ([{Item::atom(), Info::term()}], ItemInfo::dictionary) -> [{Key::term(), Value::term()}].
dump_extract_from_list(List, Key) ->
    element(2, lists:keyfind(Key, 1, List)).

%% @doc Returns a list of all currently executed functions and the number of
%%      instances for each of them.
-spec dump() -> [{Fun::mfa(), FunExecCount::pos_integer()}].
dump() ->
    Info = [element(2, Fun) || X <- processes(),
                               Fun <- [process_info(X, current_function)],
                               Fun =/= undefined],
    FunCnt = dict:to_list(lists:foldl(fun(Fun, DictIn) ->
                                              dict:update_counter(Fun, 1, DictIn)
                                      end, dict:new(), Info)),
    lists:reverse(lists:keysort(2, FunCnt)).

%% @doc Returns information about all processes' memory usage.
-spec dump2() -> [{PID::pid(), [pos_integer() | mfa() | any()]}].
dump2() ->
    dumpX([memory, current_function, dictionary],
          fun(K, Value) ->
                  case K of
                      dictionary -> util:extract_from_list_may_not_exist(Value, test_server_loc);
                      _          -> Value
                  end
          end).

%% @doc Returns various data about all processes.
-spec dump3() -> [{PID::pid(), [Mem | MsgQLength | StackSize | HeapSize | Messages | Fun]}]
        when is_subtype(Mem, non_neg_integer()),
             is_subtype(MsgQLength, non_neg_integer()),
             is_subtype(StackSize, non_neg_integer()),
             is_subtype(HeapSize, non_neg_integer()),
             is_subtype(Messages, [atom()]),
             is_subtype(Fun, mfa()).
dump3() ->
    dumpX([memory, message_queue_len, stack_size, heap_size, messages, current_function],
          fun(K, Value) ->
                  case K of
                      messages -> [element(1, V) || V <- Value];
                      _        -> Value
                  end
          end).

-spec default_dumpX_val_fun(K::atom(), Value::term()) -> term().
default_dumpX_val_fun(K, Value) ->
    case K of
        messages -> [element(1, V) || V <- Value];
        dictionary -> [element(1, V) || V <- Value];
        registered_name when Value =:= [] -> undefined;
        _        -> Value
    end.

%% @doc Returns various data about all processes.
-spec dumpX([ItemInfo::atom(),...]) -> [tuple(),...].
dumpX(Keys) ->
    dumpX(Keys, fun default_dumpX_val_fun/2).

%% @doc Returns various data about all processes.
-spec dumpX([ItemInfo::atom(),...], ValueFun::fun((atom(), term()) -> term())) -> [tuple(),...].
dumpX(Keys, ValueFun) ->
    lists:reverse(lists:keysort(2, dumpXNoSort(Keys, ValueFun))).

-spec dumpXNoSort([ItemInfo::atom(),...], ValueFun::fun((atom(), term()) -> term())) -> [tuple(),...].
dumpXNoSort(Keys, ValueFun) ->
    [{Pid, [ValueFun(Key, dump_extract_from_list(Data, Key)) || Key <- Keys]}
     || Pid <- processes(),
        undefined =/= (Data = process_info(Pid, Keys))].

%% @doc Convenience wrapper to topDumpX/3.
-spec topDumpX(Keys | Seconds | ValueFun) -> [{pid(), [Reductions | RegName | term(),...]},...]
        when is_subtype(Keys, [ItemInfo::atom()]),
             is_subtype(Seconds, pos_integer()),
             is_subtype(ValueFun, fun((atom(), term()) -> term())),
             is_subtype(Reductions, non_neg_integer()),
             is_subtype(RegName, atom()).
topDumpX(Keys) when is_list(Keys) ->
    topDumpX(Keys, fun default_dumpX_val_fun/2, 5);
topDumpX(Seconds) when is_integer(Seconds) ->
    topDumpX([], fun default_dumpX_val_fun/2, Seconds);
topDumpX(ValueFun) when is_function(ValueFun, 2) ->
    topDumpX([], ValueFun, 5).

%% @doc Gets the number of reductions for each process within the next Seconds
%%      and dumps some process data defined by Keys (sorted by the number of
%%      reductions).
-spec topDumpX(Keys, ValueFun, Seconds) -> [{pid(), [Reductions | RegName | term(),...]},...]
        when is_subtype(Keys, [ItemInfo::atom()]),
             is_subtype(Seconds, pos_integer()),
             is_subtype(ValueFun, fun((atom(), term()) -> term())),
             is_subtype(Reductions, non_neg_integer()),
             is_subtype(RegName, atom()).
topDumpX(Keys, ValueFun, Seconds) when is_integer(Seconds) andalso Seconds >= 1 ->
    Start = lists:keysort(1, dumpXNoSort([reductions, registered_name], ValueFun)),
    timer:sleep(1000 * Seconds),
    End = lists:keysort(1, dumpXNoSort([reductions, registered_name | Keys], ValueFun)),
    Self = self(),
    lists:reverse(
      lists:keysort(
        2, util:smerge2(Start, End,
                   fun(T1, T2) -> element(1, T1) =< element(1, T2) end,
                   fun(T1, T2) ->
                           % {<0.77.0>,[250004113,comm_server,692064]}
                           % io:format("T1=~.0p~nT2=~.0p~n", [T1, T2]),
                           {Pid, [Red1, RName]} = T1,
                           {Pid, [Red2, RName | Rest2]} = T2,
                           case Pid of
                               Self -> [];
                               _    -> [{Pid, [(Red2 - Red1), RName | Rest2]}]
                           end
                   end,
                   fun(_T1) -> [] end, % omit processes not alive at the end any more
                   fun(T2) -> [T2] end))).

%% @doc Convenience wrapper to topDumpXEvery/5.
-spec topDumpXEvery(Keys | Seconds | ValueFun, Subset::pos_integer(), StopAfter::pos_integer()) -> timer:tref()
        when is_subtype(Keys, [ItemInfo::atom()]),
             is_subtype(Seconds, pos_integer()),
             is_subtype(ValueFun, fun((atom(), term()) -> term())).
topDumpXEvery(Keys, Subset, StopAfter) when is_list(Keys) ->
    topDumpXEvery(Keys, fun default_dumpX_val_fun/2, 1, Subset, StopAfter);
topDumpXEvery(Seconds, Subset, StopAfter) when is_integer(Seconds) ->
    topDumpXEvery([], fun default_dumpX_val_fun/2, Seconds, Subset, StopAfter);
topDumpXEvery(ValueFun, Subset, StopAfter) when is_function(ValueFun, 2) ->
    topDumpXEvery([], ValueFun, 1, Subset, StopAfter).

%% @doc Calls topDumpX/3 every Seconds and prints the top Subset processes with
%%      the highest number of reductions. Stops after StopAfter seconds.
-spec topDumpXEvery(Keys, ValueFun, Seconds, Subset::pos_integer(), StopAfter::pos_integer()) -> timer:tref()
        when is_subtype(Keys, [ItemInfo::atom()]),
             is_subtype(Seconds, pos_integer()),
             is_subtype(ValueFun, fun((atom(), term()) -> term())).
topDumpXEvery(Keys, ValueFun, IntervalS, Subset, StopAfter)
  when is_integer(IntervalS) andalso IntervalS >= 1 andalso
           is_integer(Subset) andalso Subset >= 1 andalso
           is_integer(StopAfter) andalso StopAfter >= IntervalS ->
    {ok, TRef} = timer:apply_interval(1000 * IntervalS, ?MODULE, topDumpXEvery_helper,
                                      [Keys, ValueFun, IntervalS, Subset]),
    {ok, _} = timer:apply_after(1000 * StopAfter, timer, cancel, [TRef]),
    TRef.

%% @doc Helper function for topDumpXEvery/5 (export needed for timer:apply_after/4).
-spec topDumpXEvery_helper(Keys, ValueFun, Seconds, Subset::pos_integer()) -> ok
        when is_subtype(Keys, [ItemInfo::atom()]),
             is_subtype(Seconds, pos_integer()),
             is_subtype(ValueFun, fun((atom(), term()) -> term())).
topDumpXEvery_helper(Keys, ValueFun, Seconds, Subset) ->
    io:format("-----~n~.0p~n", [lists:sublist(topDumpX(Keys, ValueFun, Seconds), Subset)]).

-spec rr_count_old_replicas(Key::?RT:key(), Interval::intervals:interval())
        -> non_neg_integer().
rr_count_old_replicas(Key, ReqI) ->
    NodeDetails = rr_count_old_replicas_nd(Key),

    I = intervals:intersection(ReqI, node_details:get(NodeDetails, my_range)),
    case intervals:is_empty(I) of
        true -> 0;
        false ->
            DBList = rr_count_old_replicas_data(
                       node:pidX(node_details:get(NodeDetails, node))),
            This = comm:this(),
            lists:foldl(
              fun({KeyX, VerX}, Acc) ->
                      _ = [api_dht_raw:unreliable_lookup(K, {?read_op, This, 0, K, ?write})
                             || K <- ?RT:get_replica_keys(KeyX), K =/= KeyX],
                      % note: receive wrapped in anonymous functions to allow
                      %       ?SCALARIS_RECV in multiple receive statements
                      V1 = fun() ->
                                   trace_mpath:thread_yield(),
                                   receive ?SCALARIS_RECV({?read_op_with_id_reply,
                                                           0, _, ?ok,
                                                           ?value_dropped, V},
                                                          V)
                                   end end(),
                      V2 = fun() ->
                                   trace_mpath:thread_yield(),
                                   receive ?SCALARIS_RECV({?read_op_with_id_reply,
                                                           0, _, ?ok,
                                                           ?value_dropped, V},
                                                          V)
                                   end end(),
                      V3 = fun() ->
                                   trace_mpath:thread_yield(),
                                   receive ?SCALARIS_RECV({?read_op_with_id_reply,
                                                           0, _, ?ok,
                                                           ?value_dropped, V},
                                                          V)
                            end end(),
                      case VerX =:= lists:max([V1, V2, V3]) of
                          true -> Acc;
                          false -> Acc + 1
                      end
              end, 0, DBList)
end.

-spec rr_count_old_replicas_nd(Key::?RT:key()) -> node_details:node_details().
rr_count_old_replicas_nd(Key) ->
    api_dht_raw:unreliable_lookup(Key, {get_node_details, comm:this(),
                                        [node, my_range]}),
    trace_mpath:thread_yield(),
    receive ?SCALARIS_RECV({get_node_details_response, NodeDetails},% ->
                           NodeDetails) end.

-spec rr_count_old_replicas_data(Pid::comm:mypid()) -> [{?RT:key(), client_version()}].
rr_count_old_replicas_data(Pid) ->
    comm:send(Pid, {unittest_get_bounds_and_data, comm:this(), kv}),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
        {unittest_get_bounds_and_data_response, _Bounds, DBList, _Pred, _Succ},% ->
        DBList)
    end.
