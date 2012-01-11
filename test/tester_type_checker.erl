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

%-include("tester.hrl").
%-include("unittest.hrl").

%-spec check/4 :: (module(), atom(), non_neg_integer(), non_neg_integer()) -> bool().
check(true, {atom, true}, _ParseState) ->
    true;
check(true, bool, _ParseState) ->
    true;
check(Value, Type, ParseState) ->
    %ct:pal("new inner_check(~w, ~w)", [Value, Type]),
    inner_check(Value, Type, ParseState).

inner_check(Value, {list, InnerType}, ParseState) when is_list(Value) ->
    lists:all(fun (El) -> inner_check(El, InnerType, ParseState) end, Value);
inner_check(_Value, {typedef, tester, test_any}, _ParseState) ->
    true;
inner_check(Value, {typedef, Module, TypeName}, ParseState) ->
    case tester_parse_state:lookup_type({type, Module, TypeName}, ParseState) of
        none ->
            false;
        {value, Type} ->
            inner_check(Value, Type, ParseState)
    end;
inner_check(Value, {tuple, {typedef, tester, test_any}}, _ParseState) when is_tuple(Value) ->
    true;
inner_check(Value, {tuple, TupleType}, ParseState) when is_tuple(Value) ->
    case erlang:size(Value) =:= erlang:length(TupleType) of
        false -> false;
        true ->
            check_tuple_type(erlang:tuple_to_list(Value), TupleType, ParseState)
    end;
inner_check(Value, {union, TypeList}, ParseState) ->
    %ct:pal("inner check union ~p~n", [TypeList]),
    lists:any(fun (Type) -> inner_check(Value, Type, ParseState) end, TypeList);

inner_check(Value, {range, {integer, Min}, {integer, Max}}, _ParseState) when is_integer(Value) ->
    (Min =< Value) andalso (Max >= Value);

inner_check(Value, bool, _ParseState) when is_boolean(Value) ->
    true;
inner_check(Value, integer, _ParseState) when is_integer(Value) ->
    true;
inner_check(Value, non_neg_integer, _ParseState) when is_integer(Value) ->
    (0 =< Value);
inner_check(Value, pos_integer, _ParseState) when is_integer(Value) ->
    (0 < Value);
inner_check(Value, {integer, IntVal}, _ParseState) when is_integer(Value) ->
    Value =:= IntVal;
inner_check(Value, binary, _ParseState) when is_binary(Value) ->
    true;
inner_check(Value, atom, _ParseState) when is_atom(Value) ->
    true;
inner_check(Value, float, _ParseState) when is_float(Value) ->
    true;
inner_check(Value, pid, _ParseState) when is_pid(Value) ->
    true;
inner_check(Value, {atom, AtomValue}, _ParseState) when is_atom(Value) ->
    Value =:= AtomValue;

inner_check(_Value, _Type, _ParseState) ->
    false.

check_tuple_type([], [], _ParseState) ->
    true;
check_tuple_type(Value, [NextType | Rest] = _TupleType, ParseState) ->
    %ct:pal("check_tuple_type(~w, ~w)", [Value, _TupleType]),
    inner_check(hd(Value), NextType, ParseState)
        andalso
        check_tuple_type(tl(Value), Rest, ParseState).

