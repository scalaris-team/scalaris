%  @copyright 2010-2012 Zuse Institute Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    tester_type_checker.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @author Florian Schintke <schintke@zib.de>
%%% @doc    check whether a given value is of a given type
%%% @end
%%% Created :  10 Jan 2012 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(tester_type_checker).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id$').

-export([check/3]).

-include("tester.hrl").
%-include("unittest.hrl").

-spec check/3 :: (term(), type_spec(), tester_parse_state:state()) -> true | {false, term()}.
check(true, {atom, true}, _ParseState) ->
    true;
check(true, bool, _ParseState) ->
    true;
check(Value, Type, ParseState) ->
    case inner_check(Value, Type, [], ParseState) of
        true ->
            true;
        {false, _CheckStack} = R ->
            %ct:pal("Type check failed: ~.0p", [_CheckStack]),
            R
    end.

inner_check(Value, Type, CheckStack, ParseState) ->
%%    ct:pal("new inner_check(~.0p, ~.0p)", [Value, Type]),
    case Type of
        arity ->
            inner_check(Value, byte, CheckStack, ParseState);
        atom ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_atom/1, no_atom);
        {atom, _Atom} ->
            check_atom(Value, Type, CheckStack, ParseState);
        binary ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_binary/1, no_binary);
        bool ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_boolean/1, no_boolean);
        byte ->
            inner_check(Value, {range, {integer, 0}, {integer, 255}},
                        CheckStack, ParseState);
        float ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_float/1, no_float);
        integer ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_integer/1, no_integer);
        {integer, Int} ->
            check_basic_type_with_prop(
              Value, Type, CheckStack, ParseState,
              fun erlang:is_integer/1, {not_the_integer, Int},
              fun(X) -> Int =:= X end);
        {list, _InnerType} ->
            check_list(Value, Type, CheckStack, ParseState);
        neg_integer ->
            check_basic_type_with_prop(Value, Type, CheckStack, ParseState,
                                       fun erlang:is_integer/1, no_neg_integer,
                                       fun(X) -> 0 > X end);
        nil ->
            check_basic_type_with_prop(Value, Type, CheckStack, ParseState,
                                       fun erlang:is_list/1, no_empty_list,
                                       fun(X) -> [] =:= X end);
        node ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_atom/1, no_node);
        {nonempty_list, _InnerType} ->
            check_list(Value, Type, CheckStack, ParseState);
        nonempty_string ->
            %% see http://www.erlang.org/doc/reference_manual/typespec.html
            inner_check(Value, {nonempty_list,
                                {range, {integer, 0}, {integer, 16#10ffff}}},
                        [{Value, nonempty_string} | CheckStack], ParseState);
        non_neg_integer ->
            check_basic_type_with_prop(
              Value, Type, CheckStack, ParseState,
              fun erlang:is_integer/1, no_non_neg_integer,
              fun(X) -> 0 =< X end);
        number ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_number/1, no_number);
        pid ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_pid/1, no_pid);
        pos_integer ->
            check_basic_type_with_prop(Value, Type, CheckStack, ParseState,
                                       fun erlang:is_integer/1, no_pos_integer,
                                       fun(X) -> 0 < X end);
        {range, {integer, _Min}, {integer, _Max}} ->
            check_range(Value, Type, CheckStack, ParseState);
        {typedef, _Module, _TypeName} ->
            check_typedef(Value, Type, CheckStack, ParseState);
        {tuple, Tuple} when is_list(Tuple) ->
            check_tuple(Value, Type, CheckStack, ParseState);
        {tuple, Tuple} when is_tuple(Tuple) ->
            inner_check(Value, Tuple, CheckStack, ParseState);
        {union, _Union} ->
            check_union(Value, Type, CheckStack, ParseState);
        {builtin_type, module} ->
            inner_check(Value, atom, CheckStack, ParseState);
        _ ->
            ct:pal("Type checker: unsupported type: ~p", [Type]),
            {false, [{type_checker_unsupported_type, Type} | CheckStack]}
    end.

check_basic_type(Value, _Type, CheckStack, _ParseState,
                 TypeCheck, Report) ->
    case TypeCheck(Value) of
        true -> true;
        false -> {false, [{Value, Report} | CheckStack]}
    end.

check_basic_type_with_prop(Value, Type, CheckStack, ParseState,
                           TypeCheck, Report,
                           ValCheck) ->
    case check_basic_type(Value, Type, CheckStack, ParseState,
                          TypeCheck, Report) of
        true ->
            case ValCheck(Value) of
                true -> true;
                false -> {false, [{Value, Report} | CheckStack]}
            end;
        {false, _} = R -> R
    end.

check_typedef(_Value, {typedef, tester, test_any}, _, _) ->
    true;
check_typedef(Value, {typedef, Module, TypeName} = T,
              CheckStack, ParseState) ->
    case tester_parse_state:lookup_type({type, Module, TypeName}, ParseState) of
        none ->
            {false, [{tester_lookup_type_failed,
                      {Module, TypeName}} | CheckStack]};
        {value, InnerType} ->
            inner_check(Value, InnerType,
                        [{Value, T} | CheckStack], ParseState)
    end.

check_range(Value, {range, {integer, Min}, {integer, Max}} = T,
            CheckStack, _ParseState) ->
    case is_integer(Value) of
        true ->
            case (Min =< Value) andalso (Max >= Value) of
                true -> true;
                false ->
                    {false,
                     [{Value, not_in,
                       '[', Min, '..', Max, ']'} | CheckStack ]}
            end;
        false ->
            {false, [{Value, no_integer_in_range, T} | CheckStack]}
    end.

check_list(Value, {list, InnerType} = T, CheckStack, ParseState) ->
    case is_list(Value) of
        true ->
            check_list_iter(Value, InnerType,
                            [{Value, T} | CheckStack], ParseState, 1);
        false ->
            {false, [{Value, not_a_list, T} | CheckStack]}
    end;
check_list(Value, {nonempty_list, InnerType} = T, CheckStack, ParseState) ->
    case is_list(Value) andalso [] =/= Value of
        true ->
            check_list_iter(Value, InnerType,
                            [{Value, T} | CheckStack], ParseState, 1);
        false ->
            {false, [{Value, no_nonempty_list, T} | CheckStack]}
    end.


check_list_iter([], _Type, _CheckStack, _ParseState, _Count) ->
    true;
check_list_iter([Value | Tail], Type, CheckStack, ParseState, Count) ->
    case inner_check(Value, Type,
                     [{Value, list_element, Count, Type} | CheckStack],
                     ParseState) of
        true ->
            check_list_iter(Tail, Type, CheckStack, ParseState, Count + 1);
        {false, Stack} ->
            {false, Stack}
    end.

check_atom(Value, {atom, Atom} = T, CheckStack, _ParseState) ->
    case is_atom(Value) of
        true ->
            case Value =:= Atom of
                true -> true;
                false ->
                    {false, [{Value, not_the_atom, Atom} | CheckStack]}
            end;
        false ->
            {false, [{Value, no_atom, T} | CheckStack]}
    end.

check_tuple(Value, {tuple, Tuple} = T, CheckStack, ParseState) ->
    case is_tuple(Value) of
        true ->
            case erlang:tuple_size(Value) =:= erlang:length(Tuple) of
                true ->
                    check_tuple_iter(tuple_to_list(Value), Tuple,
                                     [{Value, T} | CheckStack], ParseState, 1);
                false ->
                    {false, [{Value, not_same_arity, T} | CheckStack]}
            end;
        false ->
            {false, [{Value, not_a_tuple, T} | CheckStack]}
    end.

check_tuple_iter([], [], _CheckStack, _ParseState, _Count) ->
    true;
check_tuple_iter([Value | Tail], [Type | TypeTail], CheckStack,
                 ParseState, Count) ->
    case inner_check(Value, Type,
                     [{Value, tuple_element, Count, Type} | CheckStack],
                     ParseState) of
        true ->
            check_tuple_iter(Tail, TypeTail, CheckStack, ParseState, Count + 1);
        {false, Stack} ->
            {false, Stack}
    end.

check_union(Value, {union, Union}, CheckStack, ParseState) ->
    case lists:foldl(
           fun(Type, Res) ->
                   case Res of
                       true -> true;
                       {false, Stack} ->
                           case inner_check(Value, Type, [], ParseState) of
                               true -> true;
                               {false, NewStack} ->
                                   {false, Stack ++ NewStack}
                           end
                   end
           end, {false, []}, Union) of
        true -> true;
        {false, UnionStack} ->
            {false, [{Value, no_union_variant_matched, UnionStack},
                     {Value, {union, Union}}| CheckStack]}
    end.


