%  @copyright 2010-2014 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    global state for tester
%% @end
%% @version $Id$
-module(tester_global_state).
-author('schuett@zib.de').
-vsn('$Id$').

-export([register_type_checker/3,
         unregister_type_checker/1,
         get_type_checker/1]).
-export([register_value_creator/4,
         unregister_value_creator/1,
         get_value_creator/1]).
-export([set_last_call/4, get_last_call/1, reset_last_call/1]).
-export([log_last_calls/0]).
-export([delete/0]).

-include("tester.hrl").
-include("unittest.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% handle global state, e.g. specific handlers
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec register_type_checker(type_spec(), module(), Fun::atom()) -> true.
register_type_checker(Type, Module, Fun) ->
    insert({type_checker, Type}, {Module, Fun}).

-spec unregister_type_checker(type_spec()) -> true | ok.
unregister_type_checker(Type) ->
    delete({type_checker, Type}).

-spec get_type_checker(type_spec()) -> failed | {module(), Fun::atom()}.
get_type_checker(Type) ->
    lookup({type_checker, Type}).

-spec register_value_creator(type_spec(), module(), Fun::atom(), Arity::non_neg_integer()) -> true.
register_value_creator(Type, Module, Function, Arity) ->
    insert({value_creator, Type}, {Module, Function, Arity}).

-spec unregister_value_creator(type_spec()) -> true | ok.
unregister_value_creator(Type) ->
    delete({value_creator, Type}).

-spec get_value_creator(type_spec()) -> failed | {module(), Fun::atom(), Arity::non_neg_integer()}.
get_value_creator(Type) ->
    lookup({value_creator, Type}).

-spec set_last_call(Thread::pos_integer(), module(), Fun::atom(), Args::list()) -> true.
set_last_call(Thread, Module, Function, Args) ->
    insert({last_call, Thread}, {Module, Function, Args}).

-spec reset_last_call(Thread::pos_integer()) -> true | ok.
reset_last_call(Thread) ->
    case ets:member(?MODULE, {last_call, Thread}) of
        true ->  delete({last_call, Thread});
        false -> ok
    end.

-spec get_last_call(Thread::pos_integer()) -> failed | {module(), Fun::atom(), Args::list()}.
get_last_call(Thread) ->
    lookup({last_call, Thread}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% create and query ets-table
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec lookup(any()) -> failed | any().
lookup(Key) ->
    try ets:lookup(?MODULE, Key) of
        [{Key, Value}] -> Value;
        [] -> failed
    catch error:badarg -> failed
    end.

-spec insert(Key::term(), Value::term()) -> true.
insert(Key, Value) ->
    try ets:insert(?MODULE, {Key, Value})
    catch error:badarg ->
              % probably the table does not exist
              create_table(),
              ets:insert(?MODULE, {Key, Value})
    end.

-spec delete(Key::term()) -> true | ok.
delete(Key) ->
    try begin
            case ets:member(?MODULE, Key) of
                true ->
                    ets:delete(?MODULE, Key);
                _ ->
                    %% unregister non registered object
                    ct:pal("you tried to unregister ~w which is not registered~nStacktrace: ~p", 
                           [Key, util:get_stacktrace()]),
                    throw({tester_global_state_delete_unregistered_object, Key})
            end
        end
    catch error:badarg ->
              % probably the table does not exist
              ct:pal("Stacktrace: ~p", [util:get_stacktrace()]),
              throw({tester_global_state_unknown_table, Key})
    end.

%% @doc Deletes the whole table (and the accompanying test)
-spec delete() -> ok.
delete() ->
    case erlang:whereis(?MODULE) of
        undefined -> ok;
        Pid when is_pid(Pid) ->
            MonitorRef = erlang:monitor(process, Pid),
            Pid ! {kill, self()},
            receive
                ok -> erlang:demonitor(MonitorRef, [flush]), ok;
                {'DOWN', MonitorRef, process, Pid, _Info1} -> ok
            end,
            ok
end.

-spec create_table() -> true.
create_table() ->
    P = self(),
    spawn(
      fun() ->
              catch(erlang:register(?MODULE, self())),
              _ = try ets:new(?MODULE, [set, public, named_table])
                  catch
                      % is there a race-condition?
                      Error:Reason ->
                          case ets:info(?MODULE) of
                              undefined ->
                                  ?ct_fail("could not create ets table for tester_global_state: ~p:~p",
                                           [Error, Reason]);
                              _ ->
                                  ok
                          end
                  end,
              P ! go,
              receive {kill, Pid} -> Pid ! ok
              end
      end),
    receive go -> true end.

-spec log_last_calls() -> ok.
log_last_calls() ->
    _ = [begin
             case tester_global_state:get_last_call(Thread) of
                 failed -> ok;
                 {Module, Function, Args} ->
                     ct:pal("Last call by tester (thread ~B):~n"
                            "~.0p:~.0p(~.0p).",
                            [Thread, Module, Function, Args])
             end
         end || Thread <- lists:seq(1, 8)],
    ok.
