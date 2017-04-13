%  @copyright 2014-2017 Zuse Institute Berlin

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
%% @doc    Value creator for Scalaris-specific types.
%% @end
%% @version $Id$
-module(tester_value_creator_scalaris).
-author('kruber@zib.de').
-vsn('$Id$').

-include("unittest.hrl").
-include("tester.hrl").

-export([create_value/3]).

-spec create_value(type_spec(), non_neg_integer(), tester_parse_state:state())
        -> {value, term()} | failed.
create_value({typedef, comm, reg_name, []}, _Size, _ParseState) ->
    ?ASSERT2(is_pid(whereis(tester_pseudo_proc)), process__tester_pseudo_proc__must_exist),
    {value, tester_pseudo_proc};
create_value({typedef, comm, erl_local_pid_with_reply_as, []} = Type, Size, ParseState) ->
    {Pid, e, _WrongPos, Env} =
        tester_value_creator:create_value_wo_scalaris(Type, Size, ParseState),
    Pos = tester_value_creator:create_value_wo_scalaris(
            {range, {integer, 2}, {integer, tuple_size(Env)}}, Size, ParseState),
    {value, {Pid, e, Pos, setelement(Pos, Env, '_')}};
create_value({typedef, comm, mypid_with_reply_as, []} = Type, Size, ParseState) ->
    {Pid, e, _WrongPos, Env} =
        tester_value_creator:create_value_wo_scalaris(Type, Size, ParseState),
    Pos = tester_value_creator:create_value_wo_scalaris(
            {range, {integer, 2}, {integer, tuple_size(Env)}}, Size, ParseState),
    {value, {Pid, e, Pos, setelement(Pos, Env, '_')}};
create_value({typedef, comm, mypid_plain, []}, _Size, _ParseState) ->
    ?ASSERT2(is_pid(whereis(tester_pseudo_proc)), process__tester_pseudo_proc__must_exist),
    case randoms:rand_uniform(0, 2) of
        0 -> {value, comm:make_global(whereis(tester_pseudo_proc))};
        1 -> {value, comm:make_global(tester_pseudo_proc)}
    end;
create_value(_Type, _Size, _ParseState) ->
    failed.
