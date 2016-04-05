%  @copyright 2010-2015 Zuse Institute Berlin

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
%% @doc    Global state for unit tests
%% @end
%% @version $Id$
-module(unittest_global_state).
-author('schuett@zib.de').

-export([lookup/1, insert/2, delete/1,
         delete/0]).
-export([register_thread/1, take_registered_threads/0]).

-include("unittest.hrl").

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

%% @doc Registers the calling process as a unit test thread.
-spec register_thread(ThreadNr::pos_integer()) -> true.
register_thread(ThreadNr) ->
    try ets:insert(?MODULE, {{thread, ThreadNr, self()}})
    catch error:badarg ->
              % probably the table does not exist
              create_table(),
              ets:insert(?MODULE, {{thread, ThreadNr, self()}})
    end.

-spec take_registered_threads() -> [{ThreadNr::pos_integer(), ThreadPid::pid()}].
take_registered_threads() ->
    try begin
            Matches = ets:match(?MODULE, {{thread, '$1', '$2'}}),
            [begin
                 delete({thread, ThreadNr, ThreadPid}),
                 {ThreadNr, ThreadPid}
             end || [ThreadNr, ThreadPid] <- Matches]
        end
    catch error:badarg -> failed
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
                    throw({unittest_global_state_delete_unregistered_object, Key})
            end
        end
    catch error:badarg ->
              % probably the table does not exist
              ct:pal("Stacktrace: ~p", [util:get_stacktrace()]),
              throw({unittest_global_state_unknown_table, Key})
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
              IsOwner =
                  try
                      _ = ets:new(?MODULE, [set, public, named_table]),
                      true
                  catch
                      % is there a race-condition?
                      Error:Reason ->
                          case ets:info(?MODULE) of
                              undefined ->
                                  ?ct_fail("could not create ets table for unittest_global_state: ~p:~p",
                                           [Error, Reason]);
                              _ -> false
                          end
                  end,
              P ! go,
              case IsOwner of
                  true ->
                      erlang:register(?MODULE, self()),
                      receive {kill, Pid} -> Pid ! ok
                      end;
                  false -> ok
              end
      end),
    receive go -> true end.
