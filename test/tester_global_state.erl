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
-export([set_last_call/4, reset_last_call/1]).
-export([log_last_calls/0]).

-include("tester.hrl").
-include("unittest.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% handle global state, e.g. specific handlers
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec register_type_checker(type_spec(), module(), Fun::atom()) -> true.
register_type_checker(Type, Module, Fun) ->
    unittest_global_state:insert({type_checker, Type}, {Module, Fun}).

-spec unregister_type_checker(type_spec()) -> true | ok.
unregister_type_checker(Type) ->
    unittest_global_state:delete({type_checker, Type}).

-spec get_type_checker(type_spec()) -> failed | {module(), Fun::atom()}.
get_type_checker(Type) ->
    unittest_global_state:lookup({type_checker, Type}).

-spec register_value_creator(type_spec(), module(), Fun::atom(), Arity::non_neg_integer()) -> true.
register_value_creator(Type, Module, Function, Arity) ->
    unittest_global_state:insert({value_creator, Type}, {Module, Function, Arity}).

-spec unregister_value_creator(type_spec()) -> true | ok.
unregister_value_creator(Type) ->
    unittest_global_state:delete({value_creator, Type}).

-spec get_value_creator(type_spec()) -> failed | {module(), Fun::atom(), Arity::non_neg_integer()}.
get_value_creator(Type) ->
    unittest_global_state:lookup({value_creator, Type}).

-spec set_last_call(Thread::pos_integer(), module(), Fun::atom(), Args::list()) -> true.
set_last_call(Thread, Module, Function, Args) ->
    unittest_global_state:insert({last_call, Thread, self()}, {Module, Function, Args}).

-spec reset_last_call(Thread::pos_integer()) -> true | ok.
reset_last_call(Thread) ->
    case unittest_global_state:lookup({last_call, Thread, self()}) of
        failed -> ok;
        _ ->  unittest_global_state:delete({last_call, Thread, self()})
    end,
    unittest_global_state:delete({thread, Thread, self()}).

-spec log_last_calls() -> ok.
log_last_calls() ->
    _ = [begin
             case unittest_global_state:lookup({last_call, ThreadNr, ThreadPid}) of
                 failed -> ok;
                 {Module, Function, Args} ->
                     ct:pal("Last call by tester (thread ~B-~p):~n"
                            "~.0p:~.0p(~.0p).",
                            [ThreadNr, ThreadPid, Module, Function, Args])
             end
         end || {ThreadNr, ThreadPid} <- unittest_global_state:take_registered_threads()],
    ok.
