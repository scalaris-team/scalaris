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
%%% @doc    check whether a given value is of a given type
%%% @end
%%% Created :  10 Jan 2012 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(tester_type_checker).

-author('schuett@zib.de').
-vsn('$Id$').

-export([check/3]).

%-include("tester.hrl").
%-include("unittest.hrl").

%-spec check/4 :: (module(), atom(), non_neg_integer(), non_neg_integer()) -> bool().
check(true, {atom, true}, ParseState) ->
    true;
check(true, bool, ParseState) ->
    true;
check(Value, Type, ParseState) ->
    %ct:pal("inner_check(~w, ~w)", [Value, Type]),
    inner_check(Value, Type, ParseState).

inner_check(Value, {list, InnerType}, ParseState) when is_list(Value) ->
    lists:all(fun (El) -> inner_check(El, InnerType, ParseState) end, Value);
inner_check(_Value, {typedef, tester, test_any}, ParseState) ->
    true;
inner_check(Value, {typedef, Module, TypeName}, ParseState) ->
    case tester_parse_state:lookup_type({type, Module, TypeName}, ParseState) of
        none ->
            false;
        {value, Type} ->
            inner_check(Value, Type, ParseState)
    end;
inner_check(Value, {tuple, {typedef, tester, test_any}}, ParseState) when is_tuple(Value) ->
    true;
inner_check(Value, {tuple, TupleType}, ParseState) when is_tuple(Value) ->
    case erlang:size(Value) =:= erlang:length(TupleType) of
        false -> false;
        true ->
            check_tuple_type(Value, TupleType, ParseState, 1)
    end;
inner_check(Value, {union, TypeList}, ParseState) ->
    %ct:pal("inner check union ~p~n", [TypeList]),
    lists:any(fun (Type) -> inner_check(Value, Type, ParseState) end, TypeList);

inner_check(Value, {range, {integer, Min}, {integer, Max}}, ParseState) when is_integer(Value) ->
    (Min =< Value) andalso (Max >= Value);

inner_check(Value, bool, ParseState) when is_boolean(Value) ->
    true;
inner_check(Value, integer, ParseState) when is_integer(Value) ->
    true;
inner_check(Value, {atom, AtomValue}, ParseState) when is_atom(Value) ->
    Value == AtomValue;

inner_check(Value, Type, ParseState) ->
    %ct:pal("failed inner_check(~w, ~w)", [Value, Type]),
    false.

check_tuple_type(Value, [NextType | Rest] = TupleType, ParseState, Idx) ->
    %ct:pal("check_tuple_type(~w, ~w)", [Value, TupleType]),
    inner_check(erlang:element(Idx, Value), NextType, ParseState)
        andalso
        check_tuple_type(Value, Rest, ParseState, Idx + 1);
check_tuple_type(Value, [], ParseState, Idx) ->
    erlang:tuple_size(Value) + 1 == Idx.

