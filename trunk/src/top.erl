% @copyright 2012 Zuse Institute Berlin,

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
%% @doc Text mode top for an Erlang VM. Just call top:top() in the eshell.
%% @end
%% @version $Id:$
-module(top).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").

-export([start_link/1]).
-export([on/2, init/1]).

-export([top/0]).

-type state() :: { pdb:tableid(),
                   boolean(), %% monitoring is on
                   boolean(), %% output is on
                   erlang_timestamp(), %% last output
                   non_neg_integer(), %% # of measures
                   non_neg_integer(), %% sort by
                   no_exclude | pid()
                 }.


-spec start_link(list()) -> {ok, pid()}.
start_link(Group) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{erlang_register, ?MODULE},
                             {pid_groups_join_as, Group, ?MODULE}]).

-spec init([]) -> state().
init([]) ->
    Candidates = [ {X, process_info(X, [initial_call])}
                   || X <- processes()],
    Leaders = [ {Pid, process_info(Pid, [registered_name])}
                 || {Pid, [{initial_call, Call}]} <- Candidates,
                    Call =:= {group, server, 3}],
    [Leader] = [ Pid ||  {Pid, [{registered_name, []}]} <- Leaders],
    erlang:group_leader(Leader, self()),
    {pdb:new(?MODULE, [set, protected]), false, false,
     os:timestamp(), 0, _SortBy = 3, self()}.

-spec top() -> ok.
top() ->
    io:format("~p~n", [io:rows(standard_io)]),
    Pid = pid_groups:find_a(?MODULE),
    comm:send_local(Pid, {mon_on}),
    comm:send_local(Pid, {output_on}),
    comm:send_local(Pid, {start_mon}),
    top_interactive(Pid),
    comm:send_local(Pid, {output_off}),
    comm:send_local(Pid, {stop_mon}).

top_interactive(Pid) ->
    {ok, [Char]} = io:fread(">", "~c"),
    Quit =
        case Char of
            "q" -> quit;
            "e" ->
                comm:send_local(Pid, {output_off}),
                Expr = io:parse_erl_exprs('expr>'),
                comm:send_local(Pid, {output_on}),
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
            "g" -> [garbage_collect(X) || X <- processes()];
            _ -> ok
    end,
    case Quit of
        quit -> ok;
        _ -> top_interactive(Pid)
    end.

usage() ->
    io:format(
      "~n"
      " (q)uit, (e)val expr, (g)arb-coll."
      %% ", exclude (t)op itself" %% undocumented feature for top development
      "~n sort by: (c)pu usage (m)emory (p)ids (w)aiting messages~n").

-spec on(comm:message(), state()) -> state().
on({mon_on}, State) ->
    set_monitoring(State, true);

on({output_on}, State) ->
    comm:send_local(self(), {output_data}),
    set_output(State, true);

on({output_off}, State) ->
    comm:send_local(self(), {output_data}),
    set_output(State, false);

on({sort_by, Type}, State) ->
    set_sort_by(State, Type);

on({toggle_exclude_self}, State) ->
    toggle_exclude_self(State);

on({start_mon}, State) when element(2, State) ->
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
    comm:send_local_after(1, self(), {start_mon}),
    inc_counter(State);
on({start_mon}, State) -> State;

on({output_data}, State) when element(3, State) ->
    TableName = table(State),
    %%Now = os:timestamp(),
    %%Delay = timer:now_diff(Now, timestamp(State)) / 1000000,
    Count = counter(State),
    ProcsMem = erlang:memory(processes),
    ProcsMemUsed = erlang:memory(processes_used),
    R1 = pdb:tab2list(TableName),
    PlotData = [ begin
                     Pid = element(1, X),
                     pdb:delete(Pid, TableName),
                     %% plot data is:
                     Status = try element(2, process_info(Pid, status))
                              catch _:_ -> dead_pid
                              end,
                     {vals_pid(X),
                      Status,
                      vals_mqlen(X)/Count,
                      vals_cpu_usage(X, Count),
                      try element(2, process_info(Pid, memory))
                      catch _Level2:_Reason2 -> 0
                      end}
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
    io:format("Mem: ~s procs, ~s atom, ~s binary, ~s ets~n",
             [readable_mem(ProcsMem),
              readable_mem(erlang:memory(atom)),
              readable_mem(erlang:memory(binary)),
              readable_mem(erlang:memory(ets))
             ]),
    io:format("FPS: ~4B",
             [counter(State)]),
    io:format("~n~11s ~1s ~1s ~-20s ~9s ~6s ~6s ~5s~n",
              [ "PID", "T", "S", "NAME", "W-Msg", "%CPU", "%MEM", "MEM" ]),
    _ = [ begin
          InitialCall = try element(2, process_info(Pid, initial_call))
                        catch _Level2:_Reason2 -> dead_pid
                        end,
          Name = case pid_groups:group_and_name_of(Pid) of
                     failed ->
                         try element(2, process_info(Pid, registered_name))
                         catch _Level:_Reason -> InitialCall
                         end;
                     N -> element(2, N)
                 end,
          Type = case catch(gen_component:is_gen_component(Pid)) of
                     true -> "G";
                     _ -> "E"
                 end,
          Status = case Status1 of
                       exiting -> "E";
                       garbage_collecting -> "G";
                       waiting -> "W";
                       runnable -> "I";
                       suspended -> "S";
                       running -> "R";
                       dead_pid -> "D"
                   end,
          NameString = case is_list(Name) of
                           false -> io_lib:write(Name);
                           true -> Name
                       end,
          io:format("~11w ~1s ~1s ~-20s ~9.2f ~6.2f ~6.2f ~5s~n",
                    [Pid, Type, Status,
                     NameString, MQLen, CPUUsage,
                     MemUsage / ProcsMemUsed * 100,
                     readable_mem(MemUsage)])
      end
      || {Pid, Status1, MQLen, CPUUsage, MemUsage} <- lists:reverse(PlotThese)],
    usage(),
    msg_delay:send_local(2, self(), {output_data}),
    S1 = set_counter(State, 0),
    set_timestamp(S1, os:timestamp());

on({output_data}, State) -> State;

on({stop_mon}, State) ->
    TableName = table(State),
    R1 = pdb:tab2list(TableName),
    _ = [ pdb:delete(element(1, X), TableName) || {_,_} = X <- R1 ],
    set_monitoring(State, false).


table(State) -> element(1, State).
set_monitoring(State, Val) -> setelement(2, State, Val).
set_output(State, Val) -> setelement(3, State, Val).
%% timestamp(State) -> element(4, State).
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
    (Runs + Runnable) / Count * 100.

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
             element(2, lists:keyfind(message_queue_len, 1, Vals)),
             {_E = 0,_G = 0, _W = 0, _I = 0, _S = 0, _R= 0, _D = 0} %% status
            },
    vals_add_status(Entry, element(2, lists:keyfind(status, 1, Vals))).


pdbe_add_vals(Entry, Vals) ->
    MQLen = element(2, lists:keyfind(message_queue_len, 1, Vals)),
    V1 = case MQLen > 1 of
             false -> Entry;
             true -> vals_add_mqlen(Entry, MQLen)
         end,
    Status = element(2, lists:keyfind(status, 1, Vals)),
    _V2 = vals_add_status(V1, Status).

readable_mem(Mem) ->
    case math:log(Mem)/math:log(2) of
        L when L < 10.0 ->
            "B";
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
