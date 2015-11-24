% @copyright 2012-2015 Zuse Institute Berlin,

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc    Text mode top for an Erlang VM. Just call top:top() in the eshell.
%% @end
%% @version $Id$
-module(top).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").

-behaviour(gen_component).

-export([start_link/1]).
-export([on/2, init/1]).

-export([top/0]).
-export([trace_fwd/2]).

-include("gen_component.hrl").

-type state() :: { pdb:tableid(),
                   false | all | pid, %% sampling: no, all or a single pid
                   false | all | pid, %% output: no, all, or a single pid
                   erlang_timestamp(), %% last output
                   non_neg_integer(), %% # of measures
                   non_neg_integer(), %% sort by
                   no_exclude | pid()
                 }.

-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(Group) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{erlang_register, ?MODULE},
                              {pid_groups_join_as, Group, ?MODULE}]).

-spec init([]) -> state().
init([]) ->
    try
        Candidates = [ {X, process_info(X, [initial_call])}
                       || X <- processes()],
        Leaders = [ {Pid, process_info(Pid, [registered_name])}
                    || {Pid, [{initial_call, Call}]} <- Candidates,
                       Call =:= {group, server, 3}],
        [Leader] = [ Pid ||  {Pid, [{registered_name, []}]} <- Leaders],
        erlang:group_leader(Leader, self())
    catch _:_ -> ok
    end,
    {pdb:new(?MODULE, [set]), false, false,
     os:timestamp(), 0, _SortBy = 4, self()}.

-spec top() -> ok.
top() ->
    io:format("~p~n", [io:rows(standard_io)]),
    Pid = pid_groups:find_a(?MODULE),
    comm:send_local(Pid, {enable_sampling, all}),
    comm:send_local(Pid, {sample_all}),
    comm:send_local(Pid, {enable_output, all}),
    comm:send_local(Pid, {output_all}),
    top_interactive(Pid).

top_interactive(Pid) ->
    {ok, [Char]} = io:fread("> ", "~c"),
    Quit =
        case Char of
            "q" -> quit;
            "e" ->
                comm:send_local(Pid, {output_off}),
                Expr = io:parse_erl_exprs('expr>'),
                comm:send_local(Pid, {enable_output, all}),
                comm:send_local(Pid, {output_all}),
                case element(1, Expr) of
                    ok ->
                        spawn(fun() -> erl_eval:exprs(element(2, Expr), erl_eval:new_bindings()) end),
                          ok;
                    _ -> ok
                end;
            "p" -> comm:send_local(Pid, {sort_by, pid});
            "w" -> comm:send_local(Pid, {sort_by, mqlen});
            "c" -> comm:send_local(Pid, {sort_by, cpuusage});
            "m" -> comm:send_local(Pid, {sort_by, memory});
            "t" -> comm:send_local(Pid, {toggle_exclude_self});
            "i" -> top_inspect_pid(Pid);
            "g" -> [garbage_collect(X) || X <- processes()];
            _ -> ok
    end,
    case Quit of
        quit ->
            comm:send_local(Pid, {output_off}),
            comm:send_local(Pid, {stop_sampling});
        _ -> top_interactive(Pid)
    end.

top_inspect_pid(Pid) ->
    %% Read pid and start inspect mode
    comm:send_local(Pid, {output_off}),
    comm:send_local(Pid, {stop_sampling}),
    InspectPid = pid_prompt(),
    comm:send_local(Pid, {enable_sampling, InspectPid}),
    comm:send_local(Pid, {sample_pid, InspectPid}),
    comm:send_local(Pid, {enable_output, pid}),
    comm:send_local(Pid, {output_pid, InspectPid}),
    top_inspect_pid_interactive(Pid, InspectPid),
    comm:send_local(Pid, {stop_sampling}),
    comm:send_local(Pid, {enable_sampling, all}),
    comm:send_local(Pid, {sample_all}),
    comm:send_local(Pid, {enable_output, all}),
    comm:send_local(Pid, {output_all}).

pid_prompt() ->
    {ok, [InspectPidString]} = io:fread("pid or name > ", "~s"),
    try list_to_pid(InspectPidString)
    catch _:_ ->
            try
                InspectPidAtom = list_to_existing_atom(InspectPidString),
                case whereis(InspectPidAtom) of
                    undefined ->
                        case pid_groups:find_all(InspectPidAtom) of
                            [Pid] -> Pid;
                            [] -> throw({no_pid, no_pid});
                            Pids ->
                                SPids = lists:sort(Pids),
                                io:format("Which of the ~ps do you mean?~n",
                                          [InspectPidAtom]),
                                lists:foldl(
                                  fun(X, Acc) ->
                                          io:format(
                                            " ~p: ~p ~p~n",
                                            [Acc, X,
                                             pid_groups:group_and_name_of(X)]),
                                          1 + Acc end, 1, SPids),
                                {ok, [Nth]} = io:fread("num > ", "~d"),
                                lists:nth(Nth, SPids)
                        end;
                    Pid -> Pid
                end
            catch _:_ ->
                    io:format("No such pid found. Please retry.~n"),
                    pid_prompt()
            end
    end.

top_inspect_pid_interactive(TopPid, Pid) ->
    {ok, [Char]} = io:fread("> ", "~c"),
    Quit =
        case Char of
            "q" -> quit;
            "d" -> %% show dictionary
                comm:send_local(TopPid, {output_off}),
                comm:send_local(TopPid, {stop_sampling}),
                print_process_dictionary(Pid),
                _ = io:get_chars("Hit return to continue> ", 1),
                comm:send_local(TopPid, {enable_sampling, Pid}),
                comm:send_local(TopPid, {sample_pid, Pid}),
                comm:send_local(TopPid, {enable_output, pid}),
                comm:send_local(TopPid, {output_pid, Pid}),
                ok;
            "m" -> %% show messages
                comm:send_local(TopPid, {output_off}),
                comm:send_local(TopPid, {stop_sampling}),
                print_process_messages(Pid),
                _ = io:get_chars("Hit return to continue> ", 1),
                comm:send_local(TopPid, {enable_sampling, Pid}),
                comm:send_local(TopPid, {sample_pid, Pid}),
                comm:send_local(TopPid, {enable_output, pid}),
                comm:send_local(TopPid, {output_pid, Pid}),
                ok;
            "e" ->
                comm:send_local(TopPid, {output_off}),
                Expr = io:parse_erl_exprs('expr>'),
                comm:send_local(TopPid, {enable_output, pid}),
                comm:send_local(TopPid, {output_pid, Pid}),
                case element(1, Expr) of
                    ok ->
                        spawn(fun() -> erl_eval:exprs(element(2, Expr), erl_eval:new_bindings()) end),
                          ok;
                    _ -> ok
                end;
            "c" -> comm:send_local(TopPid, {sort_by, cpuusage});
            "g" -> [garbage_collect(X) || X <- processes()];
            _ -> ok
        end,
    case Quit of
        quit ->
            comm:send_local(TopPid, {output_off}),
            comm:send_local(TopPid, {stop_sampling}),
            ok;
        _ -> top_inspect_pid_interactive(TopPid, Pid)
    end.

usage() ->
    io:format(
      "~n"
      " (q)uit, (e)val expr, (g)arb-coll. (i)nspect a pid"
      %% ", exclude (t)op itself" %% undocumented feature for top development
      "~n sort by: (c)pu usage (m)emory (p)ids (w)aiting messages~n").

usage_inspect() ->
    io:format(
      "~n"
      " (q)uit, (e)val expr, (g)arb-coll, (d)ictionary, (m)essages."
      %% ", exclude (t)op itself" %% undocumented feature for top development
      "~n sort by: (c)pu usage~n").

print_process_dictionary(Pid) ->
    Infos = try_process_info(
              Pid, [dictionary, registered_name]),

    io:format("Pid: ~p~n", [ Pid ]),
    io:format("Name: ~p",
              [process_info_get(Infos, registered_name, [])]),
    case gen_component:is_gen_component(Pid) of
        true -> io:format(" ~p", [readable_grp_and_pid_name(Pid)]);
        false -> ok
    end,
    io:format("~n"),
    io:format("Process Dictionary:~n"),
    {ok, Chars} = io:columns(), %% terminal columns
    CharsForVal = Chars - 21 - 1,
    io:format("~20s ~-" ++ integer_to_list(CharsForVal) ++ "s~n",
              ["Key", "Value"]),
    _ = [ io:format("~20s ~-" ++ integer_to_list(CharsForVal) ++ "s~n",
                    [ lists:flatten(io_lib:format("~1210.0p",
                                                  [element(1, X)])),
                      lists:flatten(io_lib:format("~111610.0p",
                                                  [element(2, X)]))])
          || X <- process_info_get(Infos, dictionary, [])],
    ok.

print_process_messages(Pid) ->
    Infos = try_process_info(
              Pid, [messages, registered_name]),

    io:format("Pid: ~p~n", [ Pid ]),
    io:format("Name: ~p",
              [process_info_get(Infos, registered_name, [])]),
    case gen_component:is_gen_component(Pid) of
        true -> io:format(" ~p", [readable_grp_and_pid_name(Pid)]);
        false -> ok
    end,
    io:format("~n"),
    {ok, Chars} = io:columns(), %% terminal columns
    {MsgCount, AllMessages0} =
        lists:foldl(fun(MsgX, {I, TreeX}) ->
                            % note: don't use comm:get_msg_tag/1 - we want to keep the group_message tags
                            Tag = erlang:element(1, MsgX),
                            Size = erlang:byte_size(erlang:term_to_binary(MsgX, [{minor_version, 1}])),
                            case gb_trees:lookup(Tag, TreeX) of
                                none ->
                                    MsgX2 = prettyprint_msg(MsgX),
                                    {I + 1, gb_trees:insert(Tag, {1, I, I, Size, MsgX2}, TreeX)};
                                {value, {OldCount, OldFirstPos, _OldLastPos, OldSize, OldMsg}} ->
                                    {I + 1, gb_trees:update(Tag, {OldCount + 1, OldFirstPos, I, Size + OldSize, OldMsg}, TreeX)}
                            end
                    end, {0, gb_trees:empty()}, process_info_get(Infos, messages, [])),
    AllMessages = lists:reverse(
                    lists:keysort(1, util:gb_trees_foldl(
                                    fun(_Tag, {TagCnt, TagFirstPos, TagLastPos, TagSize, Example}, L) ->
                                            [{TagCnt, TagFirstPos, TagLastPos, TagSize, Example} | L]
                                    end, [], AllMessages0))),
    io:format("Process Messages (total ~p):~n", [MsgCount]),
    CharsForVal = Chars - 7 - 9 - 13 - 1,
    io:format("~6s ~8s ~12s ~-" ++ integer_to_list(CharsForVal) ++ "s~n",
              ["Count", "Size", "Positions", "First Message"]),
    _ = [begin
             FirstMessage = lists:flatten(io_lib:format("~111610.0p", [Example])),
             {FirstMessage1, FirstMessage2} = util:safe_split(CharsForVal, FirstMessage),
             {AddStr, AddVal} =
                 case FirstMessage2 of
                     [] -> {[], []};
                     _  ->
                         {FirstMessage2a, FirstMessage2b} = util:safe_split(CharsForVal - 1, FirstMessage2),
                         case FirstMessage2b of
                             [] ->
                                 {"~29s ~-" ++ integer_to_list(CharsForVal-1) ++ "s~n",
                                  ["", FirstMessage2a]};
                             _  ->
                                 {"~29s ~-" ++ integer_to_list(CharsForVal-1) ++ "s~n"
                                  "~29s ~-" ++ integer_to_list(CharsForVal-1) ++ "s~n",
                                  ["", FirstMessage2a, "", FirstMessage2b]}
                         end
                 end,
             io:format("~6s ~7sk ~12s ~-" ++ integer_to_list(CharsForVal) ++ "s~n" ++ AddStr,
                       [ lists:flatten(io_lib:format("~1210.0p", [TagCnt])),
                         lists:flatten(io_lib:format("~1210.0p", [TagSize div 1024])),
                         lists:flatten(io_lib:format("~1210.0p-~1210.0p", [TagFirstPos, TagLastPos])),
                         FirstMessage1] ++ AddVal)
         end
            || {TagCnt, TagFirstPos, TagLastPos, TagSize, Example} <- AllMessages],
    ok.

-spec prettyprint_msg(comm:message()) -> comm:message().
prettyprint_msg({?lookup_aux, Key, Hops, Msg}) ->
    {util:extint2atom(?lookup_aux), Key, Hops, prettyprint_msg(Msg)};
prettyprint_msg({?lookup_fin, Key, Hops, Msg}) ->
    {util:extint2atom(?lookup_fin), Key, Hops, prettyprint_msg(Msg)};
prettyprint_msg({?send_to_group_member, ProcessName, Msg}) ->
    {util:extint2atom(?send_to_group_member), ProcessName, prettyprint_msg(Msg)};
prettyprint_msg({?send_to_registered_proc, ProcessName, Msg}) ->
    {util:extint2atom(?send_to_registered_proc), ProcessName, prettyprint_msg(Msg)};
prettyprint_msg(Msg) ->
    case Msg of
        {X, f, Y} when is_integer(X) andalso is_tuple(Y) ->
            %% handle lookup envelope separately
            {X, f, prettyprint_msg(Y)};
        _ ->
            setelement(1, Msg, util:extint2atom(element(1, Msg)))
    end.

-spec on(comm:message(), state()) -> state().

on({enable_sampling, Val}, State) ->
    case is_pid(Val) of
        true ->
            dbg:stop_clear(),
            dbg:tracer(process, {fun top:trace_fwd/2, self()}),
            _ = dbg:tpl('_', []),
            dbg:p(Val, [c, return_to]);
        false -> ok
    end,
    set_sampling(State, Val);

on({enable_output, Val}, State)   -> set_output(State, Val);
on({sort_by, Type}, State)        -> set_sort_by(State, Type);
on({toggle_exclude_self}, State)  -> toggle_exclude_self(State);
on({output_off}, State)           -> set_output(State, false);

%% gather information on all local pids for periodic statistical output
on({sample_all}, State) when all =:= element(2, State) ->
    T = [ catch({X, erlang:process_info(X, [message_queue_len, status])})
          || X <- erlang:processes(), X =/= exclude_self(State) ],

    TableName = table(State),
    _ = [ begin
          Y = pdb:get(P, TableName),
          case Y of
              undefined ->
                  V1 = pdbe_new(P, NewVals),
                  pdb:set(V1, TableName);
              _ ->
%%                  io:format("X:~p Y:~p~n", [X, Y]),
                  V1 = pdbe_add_vals(Y, NewVals),
                  pdb:set(V1, TableName)
          end
      end || {P, NewVals} <- T, is_pid(P), undefined =/= NewVals],
    _ = comm:send_local_after(1, self(), {sample_all}),
    inc_counter(State);

on({sample_all}, State) -> State;

%% gather information on a single pid for periodic statistical output
on({sample_pid, Pid}, State) when is_pid(element(2, State)) ->
    TableName = table(State),
    NewVals = case try_process_info(
                     Pid,
                     [message_queue_len, status]) of
                  undefined -> [];
                  Infos -> Infos
              end,
    TakeVals = case process_info_get(NewVals, status, dead_pid) of
                   running -> true;
                   runnable -> true;
                   _ -> false
               end,
    %% _ = comm:send_local_after(1, self(), {sample_pid, Pid}),
    comm:send_local(self(), {sample_pid, Pid}),
    case TakeVals of
        true ->
            %P = {Pid, process_info_get(NewVals, current_function, dead)},
            P = {Pid, erlang:get(current_function)},
            Entry = pdb:get(P, TableName),
            case Entry of
                undefined ->
                    V1 = pdbe_new(P, NewVals),
                    pdb:set(V1, TableName);
                _ ->
                    %% io:format("X:~p Y:~p~n", [X, Y]),
                    V1 = pdbe_add_vals(Entry, NewVals),
                    pdb:set(V1, TableName)
            end;
        false ->
            P = {Pid, not_running},
            Entry = pdb:get(P, TableName),
            TVals = lists:keyreplace(status, 1,
                                     NewVals, {status, runnable}),
            case Entry of
                undefined ->
                    V1 = pdbe_new(P, TVals),
                    pdb:set(V1, TableName);
                _ ->
                    V1 = pdbe_add_vals(Entry, TVals),
                    pdb:set(V1, TableName)
            end
    end,
    inc_counter(State);

on({sample_pid, _Pid}, State) -> State;

on({trace, _Pid, Fun}, State) ->
    erlang:put(current_function, Fun),
    State;

on({output_all}, State) when all =:= element(3, State) ->
    TableName = table(State),
    Now = os:timestamp(),
    Delay = timer:now_diff(Now, timestamp(State)) / 1000000,
    Count = counter(State),
    ProcsMem = erlang:memory(processes),
    ProcsMemUsed = erlang:memory(processes_used),
    R1 = pdb:tab2list(TableName),
    PlotData = [ begin
                     Pid = element(1, X),
                     pdb:delete(Pid, TableName),
                     %% plot data is:
                     Infos = try_process_info(Pid, [status, memory, stack_size]),
                     Status = process_info_get(Infos, status, dead_pid),
                     {vals_pid(X),
                      Status,
                      vals_mqlen(X)/Count,
                      vals_cpu_usage(X, Count),
                      process_info_get(Infos, memory, 0),
                      process_info_get(Infos, stack_size, 0)}
                 end || X <- R1, is_tuple(X), is_pid(element(1, X)) ],
    SortedPlotData = lists:keysort(sort_by(State), PlotData),
    L = length(SortedPlotData),
    {ok, Lines} = io:rows(), %% terminal rows
    {_, PlotThese} =
        lists:split(erlang:max(0, L - (Lines - 9)), SortedPlotData),
    Time = element(2, calendar:local_time()),
    WallClock = element(1, erlang:statistics(wall_clock)) div 60000,
    io:format("top - ~2..0B:~2..0B:~2..0B up ~B days, ~B:~2..0B, load average: ~.2f~n",
             [element(1, Time), element(2, Time), element(3, Time),
              WallClock div (60*24),
              WallClock rem (60*24) div 60,
              (WallClock rem (60*60)) rem 60,
              lists:foldl(fun(X, Acc) -> element(4, X)/100 + Acc end,
                          0, PlotData)
             ]),
    io:format("Tasks: ~p total, ~p running, ~p sleeping, ~p garb-coll.~n",
             [L,
              lists:foldl(fun(X, Acc) -> case element(2, X) of
                                             runnable -> 1 + Acc;
                                             running -> 1 + Acc;
                                             _ -> Acc
                                         end end, 0, PlotData),
              lists:foldl(fun(X, Acc) ->
                                  case element(2, X) of
                                      waiting -> 1 + Acc;
                                      suspended -> 1 + Acc;
                                      exiting -> 1 + Acc;
                                      dead_pid -> 1 + Acc;
                                      _ -> Acc
                                  end end, 0, PlotData),
              lists:foldl(fun(X, Acc) ->
                                  case element(2, X) of
                                      garbage_collecting -> 1 + Acc;
                                      _ -> Acc
                                  end end, 0, PlotData)
              ]),
    {{_, IOIn}, {_, IOOut}} = erlang:statistics(io),
    %% {GCNum, GCFreed, _} = erlang:statistics(garbage_collection),
    %% io:format("Context Switches: ~p Garb.Coll: ~p ~s~n",
    %%           [element(1, erlang:statistics(context_switches)),
    %%            GCNum, readable_mem(GCFreed)]),

    io:format("IO: ~s/~s in/out Mem: ~s procs, ~s atom, ~s binary, ~s ets~n",
              [readable_mem(IOIn), readable_mem(IOOut),
               readable_mem(ProcsMem),
               readable_mem(erlang:memory(atom)),
               readable_mem(erlang:memory(binary)),
               readable_mem(erlang:memory(ets))
              ]),
    io:format("FPS: ~.1f~n", [Count / Delay]),
    WordSize = erlang:system_info(wordsize),
    io:format("~12s ~1s ~1s ~-20s ~9s ~6s ~6s ~5s ~5s~n",
              [ "PID", "T", "S", "NAME", "W-Msg", "%CPU", "%MEM", "MEM", "STACK" ]),
    _ = [ begin
          Type = case catch(gen_component:is_gen_component(Pid)) of
                     true -> "G";
                     _ -> "E"
                 end,
          io:format("~12w ~1s ~1s ~-20s ~9.2f ~6.2f ~6.2f ~5s ~5s~n",
                    [Pid, Type, readable_status(Status),
                     readable_pid_name(Pid), MQLen, CPUUsage,
                     MemUsage / ProcsMemUsed * 100,
                     readable_mem(MemUsage),
                     readable_mem(WordSize * StackSize)])
      end
      || {Pid, Status, MQLen, CPUUsage, MemUsage, StackSize} <- lists:reverse(PlotThese)],
    usage(),
    msg_delay:send_trigger(2, {output_all}),
    S1 = set_counter(State, 1),
    set_timestamp(S1, os:timestamp());

on({output_all}, State) -> State;

on({output_pid, Pid}, State) when pid =:= element(3, State) ->
    TableName = table(State),
    Now = os:timestamp(),
    Delay = timer:now_diff(Now, timestamp(State)) / 1000000,
    Count = counter(State),
    Infos = try_process_info(
              Pid, [dictionary, registered_name, memory, total_heap_size,
                    stack_size, status, message_queue_len, current_function]),
    PD = process_info_get(Infos, dictionary, []),
    R1 = pdb:tab2list(TableName),
    PlotData = [ begin
                     pdb:delete(PidAndCFun, TableName),
                     %% plot data is:
                     {PidAndCFun,
                      no_status,
                      no_mqlen,
                      vals_cpu_usage(X, Count),
                      no_mem}
                 end || {PidAndCFun, MQLen, Status} = X <- R1,
                        is_tuple(Status), is_number(MQLen)
               ],
    SortedPlotData = lists:keysort(sort_by(State), PlotData),
    L = length(SortedPlotData),
    {ok, Lines} = io:rows(), %% terminal rows
    SkipLines = 11,
    {_, PlotThese} =
        lists:split(erlang:max(0, L - (Lines - SkipLines)), SortedPlotData),

    Time = element(2, calendar:local_time()),
    WallClock = element(1, erlang:statistics(wall_clock)) div 60000,
    io:format("top - ~2..0B:~2..0B:~2..0B up ~B days, ~B:~2..0B~n",
              [element(1, Time), element(2, Time), element(3, Time),
               WallClock div (60*24),
               WallClock rem (60*24) div 60,
               (WallClock rem (60*60)) rem 60
              ]),
    IdlePlotDataEntry = lists:keyfind({Pid, not_running}, 1, PlotData),
    Idle = case IdlePlotDataEntry of
        false -> 0.0;
        _ -> element(4, IdlePlotDataEntry)
    end,
    io:format("Pid: ~p, ~6.2f% run, ~6.2f% idle~n",
              [ Pid, 100.0 - Idle, Idle ]),
    io:format("Name: ~p",
              [process_info_get(Infos, registered_name, [])]),
    case gen_component:is_gen_component(Pid) of
        true -> io:format(" ~p", [readable_grp_and_pid_name(Pid)]);
        false -> ok
    end,
    io:format("~n"),
    MemUsage = process_info_get(Infos, memory, 0),
    HeapSize = process_info_get(Infos, total_heap_size, 0),
    StackSize = process_info_get(Infos, stack_size, 0),
    PDSize = erlang:byte_size(erlang:term_to_binary(PD, [{minor_version, 1}])),
    WordSize = erlang:system_info(wordsize),
    io:format("Mem: ~s total, ~s heap, ~s stack, ~s dictionary (~p entries)~n",
              [readable_mem(MemUsage),
               readable_mem((HeapSize - StackSize) * WordSize),
               readable_mem(StackSize * WordSize),
               readable_mem(PDSize), length(PD)
              ]),

    io:format("Status: ~s MQLen: ~p Current Function: ~p~n",
              [readable_status(
                 process_info_get(Infos, status, dead_pid)),
               process_info_get(Infos, message_queue_len, 0),
               erlang:get(current_function)
              ]),
    io:format("FPS: ~.1f~n", [Count / Delay]),

    io:format("~12s ~-35s ~6s~n",
              [ "PID", "FUNCTION", "%RTIME"]),
    _ = [ begin
              CFun = element(2, PlotPid),
              CFunString =
                  case is_tuple(CFun) of
                      true ->
                          io_lib:write(element(1, CFun))
                              ++ ":" ++ io_lib:write(element(2, CFun))
                              ++ "/" ++ io_lib:write(element(3, CFun));
                      false -> io_lib:write(CFun)
                  end,
              FunRTime = if 0.004 > (100.0 - Idle) ->
                                 case L > 1 of
                                     true -> 100.0 / (L-1);
                                     false -> 100.0
                                 end;
                            true -> CPUUsage * 100.0 / (100.0 - Idle)
                         end,
              io:format("~12w ~-35s ~6.2f~n",
                        [element(1, PlotPid), CFunString, FunRTime ])
          end
          || {PlotPid, _Status, _MQLen, CPUUsage, _MemUsage}
                 <- lists:reverse(PlotThese), not_running =/= element(2, PlotPid)],
    util:for_to(1, Lines - SkipLines - length(PlotThese),
                fun(_) -> io:format("~n") end),
    usage_inspect(),
    msg_delay:send_local(4, self(), {output_pid, Pid}),
    S1 = set_counter(State, 0),
    set_timestamp(S1, os:timestamp());

on({output_pid, _Pid}, State) -> State;

on({stop_sampling}, State) ->
    TableName = table(State),
    R1 = pdb:tab2list(TableName),
    _ = [ pdb:delete(element(1, X), TableName)
          || {_,_,_} = X <- R1, is_pid(element(1, X)) ],
    S1 = set_counter(State, 0),
    dbg:stop_clear(),
    set_sampling(S1, false).


table(State) -> element(1, State).
set_sampling(State, Val) -> setelement(2, State, Val).
set_output(State, Val) -> setelement(3, State, Val).
timestamp(State) -> element(4, State).
set_timestamp(State, Val) -> setelement(4, State, Val).
counter(State) -> element(5, State).
inc_counter(State) -> setelement(5, State, 1 + element(5, State)).
set_counter(State, Val) -> setelement(5, State, Val).
sort_by(State) -> element(6, State).
set_sort_by(State, Type) ->
    Val = case Type of
              pid -> 1;
              mqlen -> 3;
              cpuusage -> 4;
              memory -> 5
          end,
    setelement(6, State, Val).
exclude_self(State) -> element(7, State).
toggle_exclude_self(State) ->
    Self = self(),
    case element(7, State) of
        Self -> setelement(7, State, no_exclude);
        _ -> setelement(7, State, self())
    end.

vals_pid(Entry) -> element(1, Entry).
vals_mqlen(Entry) -> element(2, Entry).

vals_cpu_usage(Entry, Count) ->
    Status = element(3, Entry),
    Runs = element(6, Status),
    Runnable = element(4, Status),
    case Count of
        0 -> 0;
        _ -> (Runs + Runnable) / Count * 100
    end.

vals_add_mqlen(Entry, Amount) ->
    setelement(2, Entry, Amount + element(2, Entry)).
vals_add_status(Entry, Val) ->
    OldStatus = element(3, Entry),
    NewStatus =
        case Val of
            exiting ->
                setelement(1, OldStatus, 1 + element(1, OldStatus));
            garbage_collecting ->
                setelement(2, OldStatus, 1 + element(2, OldStatus));
            waiting            ->
                setelement(3, OldStatus, 1 + element(3, OldStatus));
            runnable           ->
                setelement(4, OldStatus, 1 + element(4, OldStatus));
            suspended          ->
                setelement(5, OldStatus, 1 + element(5, OldStatus));
            running            ->
                setelement(6, OldStatus, 1 + element(6, OldStatus));
            dead_pid           ->
                setelement(7, OldStatus, 1 + element(7, OldStatus))
        end,
    setelement(3, Entry, NewStatus).

pdbe_new(Pid, Vals) ->
    Entry = {Pid,
             process_info_get(Vals, message_queue_len, 0),
             {_E = 0,_G = 0, _W = 0, _I = 0, _S = 0, _R= 0, _D = 0} %% status
            },
    vals_add_status(Entry, process_info_get(Vals, status, dead_pid)).


pdbe_add_vals(Entry, Vals) ->
    MQLen = process_info_get(Vals, message_queue_len, 0),
    V1 = case MQLen > 1 of
             false -> Entry;
             true -> vals_add_mqlen(Entry, MQLen)
         end,
    Status = process_info_get(Vals, status, dead_pid),
    _V2 = vals_add_status(V1, Status).

readable_status(Status) ->
    case Status of
        exiting -> "E";
        garbage_collecting -> "G";
        waiting -> "W";
        runnable -> "R";
        suspended -> "S";
        running -> "R";
        dead_pid -> "D"
    end.

readable_mem(0) -> "0B";
readable_mem(Mem) ->
    case math:log(Mem)/math:log(2) of
        L when L < 10.0 ->
            lists:flatten(io_lib:format("~p", [Mem]))
            ++ "B";
        L when L < 20.0 ->
            lists:flatten(io_lib:format("~p", [Mem div 1024]))
            ++ "KB";
        L when L < 30.0 ->
            lists:flatten(io_lib:format("~p", [Mem div 1024 div 1024]))
            ++ "MB";
        L when L >= 30.0 ->
            lists:flatten(
              io_lib:format(
                "~p", [Mem div 1024 div 1024 div 1024]))
            ++ "GB"
    end.

readable_pid_name(Pid) ->
    Name = case pid_groups:group_and_name_of(Pid) of
               failed ->
                   try element(2, process_info(Pid, registered_name))
                   catch _Level:_Reason ->
                           try_process_info(Pid, initial_call, dead_pid)
                   end;
               N -> element(2, N)
           end,
    case is_list(Name) of
        false -> io_lib:write(Name);
        true -> Name
    end.

readable_grp_and_pid_name(Pid) ->
    case pid_groups:group_and_name_of(Pid) of
        failed ->
            try element(2, process_info(Pid, registered_name))
            catch _Level:_Reason ->
                    try_process_info(Pid, initial_call, dead_pid)
            end;
        N -> N
    end.

try_process_info(Pid, Types) ->
    try erlang:process_info(Pid, Types)
    catch _:_ -> []
    end.

try_process_info(Pid, Type, DefaultVal) ->
    try element(2, erlang:process_info(Pid, Type))
    catch _:_ -> DefaultVal
    end.

process_info_get(undefined, _Type, DefaultVal) ->
    DefaultVal;
process_info_get(PropList, Type, DefaultVal) ->
    case lists:keyfind(Type, 1, PropList) of
        false -> DefaultVal;
        X -> element(2, X)
    end.

-spec trace_fwd({trace, pid(), call | return_to, mfa()}, pid()) -> pid().
trace_fwd({trace, Pid, call, {Mod, Fun, Params}}, TopPid) ->
    TopPid ! {trace, Pid, {Mod, Fun, length(Params)}},
    TopPid;
trace_fwd({trace, Pid, return_to, {Mod, Fun, Arity}}, TopPid) ->
    TopPid ! {trace, Pid, {Mod, Fun, Arity}},
    TopPid.

