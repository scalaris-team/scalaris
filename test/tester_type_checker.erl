%  @copyright 2010-2017 Zuse Institute Berlin

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
%% @author Florian Schintke <schintke@zib.de>
%% @doc    check whether a given value is of a given type
%% @end
-module(tester_type_checker).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id$').

-export([check/3]).
-export([log_error/1]).
-export([render_type/1]).

-include("tester.hrl").
%-include("unittest.hrl").

-spec check(term(), type_spec(), tester_parse_state:state()) -> true | {false, term()}.
check(true, {atom, true}, _ParseState) ->
    true;
check(true, bool, _ParseState) ->
    true;
check(Value, Type, ParseState) ->
    case inner_check(Value, Type, [], ParseState) of
        true ->
            true;
        {false, _CheckStack} = R ->
            %%ct:pal("Type check failed: ~.0p", [_CheckStack]),
            R
    end.

inner_check(Value, Type, CheckStack, ParseState) ->
%%    ct:pal("new inner_check(~.0p, ~.0p)", [Value, Type]),
    case tester_global_state:get_type_checker(Type) of
        failed ->
            inner_check_(Value, Type, CheckStack, ParseState);
        {Module, Function} ->
            %ct:pal("using ~p:~p for checking ~p", [Module, Function, Value]),
            case inner_check_(Value, Type, CheckStack, ParseState) of
                true ->
                    case apply(Module, Function, [Value]) of
                        true ->
                            true;
                        false ->
                            {false,
                             [{Value,
                               structural_test_ok_your_registered_type_checker_failed,
                               {Module, Function}}
                              | CheckStack]}
                    end;
                X ->
                    X
            end
    end.

inner_check_(Value, Type, CheckStack, ParseState) ->
    case Type of
        arity ->
            inner_check(Value, byte, CheckStack, ParseState);
        atom ->
            check_basic_type(Value, atom, CheckStack, ParseState,
                             fun erlang:is_atom/1, no_atom);
        {atom, _Atom} ->
            check_atom(Value, Type, CheckStack, ParseState);
        {binary, []} ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_binary/1, no_binary);
        {builtin_type, bitstring} ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_bitstring/1, no_bitstring);
        bool ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_boolean/1, no_boolean);
        byte ->
            inner_check(Value, {range, {integer, 0}, {integer, 255}},
                        CheckStack, ParseState);
        {builtin_type, array_array, ValueType} ->
            case array:is_array(Value) of
                true ->
                    check_list(array:to_list(Value), % [Value]
                               {list, ValueType},
                               CheckStack, ParseState);
                false -> {false, [{Value, array_is_array_returned_false} | CheckStack]}
            end;
        {builtin_type, module} ->
            inner_check(Value, atom, CheckStack, ParseState);
        {builtin_type, dict_dict, KeyType, ValueType} ->
            % there is no is_dict/1, so try some functions on the dict to check
            try
                _ = dict:size(Value),
                _ = dict:find('$non_existing_key', Value),
                _ = dict:store('$non_existing_key', '$value', Value),
                check_list(dict:to_list(Value), % [{Key, Value}]
                           {list, {tuple, [KeyType, ValueType]}},
                           CheckStack, ParseState)
            catch _:_ -> {false, [{Value, dict_functions_thrown} | CheckStack]}
            end;
        {builtin_type, gb_trees_tree, KeyType, ValueType} ->
            % there is no is_gb_tree/1, so try some functions on the tree to check
            try
                _ = gb_trees:size(Value),
                _ = gb_trees:is_defined('$non_existing_key', Value),
                _ = gb_trees:enter('$non_existing_key', '$value', Value),
                check_list(gb_trees:to_list(Value), % [{Key, Value}]
                           {list, {tuple, [KeyType, ValueType]}},
                           CheckStack, ParseState)
            catch _:_ -> {false, [{Value, gb_trees_functions_thrown} | CheckStack]}
            end;
        {builtin_type, gb_sets_set, ValueType} ->
            % there is no is_gb_set/1, so try some functions on the set to check
            try
                _ = gb_sets:size(Value),
                _ = gb_sets:is_member('$non_existing_key', Value),
                _ = gb_sets:add('$non_existing_key', Value),
                check_list(gb_sets:to_list(Value), % [Key]
                           {list, ValueType},
                           CheckStack, ParseState)
            catch _:_ -> {false, [{Value, gb_sets_functions_thrown} | CheckStack]}
            end;
        {builtin_type, map} ->
            % there is no is_map/1, so try some functions on the map to check
            try check_map(Value, CheckStack)
            catch _:_ -> {false, [{Value, map_functions_thrown} | CheckStack]}
            end;
        float ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_float/1, no_float);
        {'fun', {product, _ParamTypes}, _ResultType} ->
            check_fun(Value, Type, CheckStack, ParseState);
        integer ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_integer/1, no_integer);
        {integer, Int} ->
            check_basic_type_with_prop(
              Value, Type, CheckStack, ParseState,
              fun erlang:is_integer/1, not_the_integer,
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
        {product, TypeList} when is_list(TypeList) ->
            check_tuple(Value, {tuple, TypeList}, CheckStack, ParseState);
        {range, {integer, _Min}, {integer, _Max}} ->
            check_range(Value, Type, CheckStack, ParseState);
        {record, _Module, _Typedef} ->
            check_record(Value, Type, CheckStack, ParseState);
        {record, FieldList} when is_list(FieldList) ->
            check_record_fields(Value, Type, CheckStack, ParseState);
        reference ->
            check_basic_type(Value, Type, CheckStack, ParseState,
                             fun erlang:is_reference/1, no_reference);
        tid ->
            % built-in < R14; otherwise ets:tid()
            inner_check(Value, integer, CheckStack, ParseState);
        {tuple, Tuple} when is_list(Tuple) ->
            check_tuple(Value, Type, CheckStack, ParseState);
        {tuple, Tuple} when is_tuple(Tuple) ->
            inner_check(Value, Tuple, CheckStack, ParseState);
        {typedef, ets, tid, []} -> % tid is reference in R20+
            inner_check_(Value, integer, CheckStack, ParseState);
        {typedef, dict, dict, []} ->
            check_list(dict:to_list(Value), % [Key,Value]
                       {list, {tuple,
                               [{typedef, tester, test_any, []}, {typedef, tester, test_any, []}]}},
                       CheckStack, ParseState);
        {typedef, orddict, orddict, []} ->
            check_list(orddict:to_list(Value), % [Key,Value]
                       {list, {tuple,
                               [{typedef, tester, test_any, []}, {typedef, tester, test_any, []}]}},
                       CheckStack, ParseState);
        {typedef, _Module, _TypeName, []} ->
            check_typedef(Value, Type, CheckStack, ParseState);
        {union, _Union} ->
            check_union(Value, Type, CheckStack, ParseState);
        {var_type, [], InnerType} ->
            % @todo add substitution
            inner_check_(Value, InnerType, CheckStack, ParseState);
        _ ->
            ct:pal("Type checker: unsupported type: ~p", [Type]),
            ?DBG_ASSERT2(false, "unknown type"),
            {false, [{type_checker_unsupported_type, Type} | CheckStack]}
    end.

check_basic_type(Value, Type, CheckStack, _ParseState,
                 TypeCheck, Report) ->
    case TypeCheck(Value) of
        true -> true;
        false -> {false, [{Value, Report, Type} | CheckStack]}
    end.

check_basic_type_with_prop(Value, Type, CheckStack, ParseState,
                           TypeCheck, Report,
                           ValCheck) ->
    case check_basic_type(Value, Type, CheckStack, ParseState,
                          TypeCheck, Report) of
        true ->
            case ValCheck(Value) of
                true -> true;
                false -> {false, [{Value, Report, Type} | CheckStack]}
            end;
        {false, _} = R -> R
    end.

check_typedef(_Value, {typedef, tester, test_any, []}, _, _) ->
    true;
check_typedef(Value, {typedef, Module, TypeName, []} = T,
              CheckStack, ParseState) ->
    case tester_parse_state:lookup_type({type, Module, TypeName, length([])}, ParseState) of
        none ->
            ct:pal("error in check_typedef ~w:~w", [Module, TypeName]),
            {false, [{tester_lookup_type_failed,
                      {Module, TypeName}} | CheckStack]};
        {value, {var_type, [], InnerType}} ->
            case inner_check(Value, InnerType,
                             CheckStack, ParseState) of
                {false, ErrStack} ->
                    {false, [{T, defined_as, InnerType, ErrStack}]};
                true -> true
            end
    end.

check_range(Value, {range, {integer, Min}, {integer, Max}} = T,
            CheckStack, _ParseState) ->
    case is_integer(Value) of
        true ->
            case (Min =< Value) andalso (Max >= Value) of
                true -> true;
                false ->
                    {false,
                     [{Value, not_in, T} | CheckStack ]}
            end;
        false ->
            {false, [{Value, no_integer_in_range, T} | CheckStack]}
    end.

check_record(Value, {record, Module, TypeName} = T, CheckStack, ParseState) ->
    case tester_parse_state:lookup_type(T, ParseState) of
        none ->
            ct:pal("error in check_record"),
            {false, [{tester_lookup_type_failed,
                      {Module, TypeName}} | CheckStack]};
        {value, {var_type, [], {record, FieldList} = _InnerType}} ->
            %% check record name here (add it as record field in front)
            case inner_check(Value, {record,
                        [ {typed_record_field, tag, {atom, TypeName}}
                          | FieldList ]},
                             CheckStack, ParseState) of
                {false, ErrStack} ->
                    {false, [{Value, record_field_mismatch, T, ErrStack}]};
                true -> true
            end
    end.


check_record_fields(Value, {record, FieldList}, CheckStack, ParseState)
  when is_list(FieldList) ->
    %% [{typed_record_field,FieldName, Type}]
    {_, _, TypeList} = lists:unzip3(FieldList),
    check_tuple(Value, {tuple, TypeList},
                CheckStack, ParseState).

check_list(Value, {list, InnerType} = T, CheckStack, ParseState) ->
    case is_list(Value) of
        true ->
            case check_list_iter(Value, InnerType, CheckStack, ParseState, 1) of
                {false, ErrStack} ->
                    {false, [{Value, list_element_mismatch, T, ErrStack}]};
                true -> true
            end;
        false ->
            {false, [{Value, no_list, T} | CheckStack]}
    end;
check_list(Value, {nonempty_list, InnerType} = T, CheckStack, ParseState) ->
    case is_list(Value) andalso [] =/= Value of
        true ->
            case check_list_iter(Value, InnerType, CheckStack, ParseState, 1) of
                {false, ErrStack} ->
                    {false, [{Value, list_element_mismatch, T, ErrStack}]};
                        true -> true
                    end;
        false ->
            {false, [{Value, no_nonempty_list, T} | CheckStack]}
    end.


check_list_iter([], _Type, _CheckStack, _ParseState, _Count) ->
    true;
check_list_iter([Value | Tail], Type, CheckStack, ParseState, Count) ->
    case inner_check(Value, Type, CheckStack, ParseState) of
        true ->
            check_list_iter(Tail, Type, CheckStack, ParseState, Count + 1);
        {false, Stack} ->
            {false, [{Value, list_element, Count, Type, Stack}]}
    end.

check_atom(Value, {atom, Atom} = T, CheckStack, _ParseState) ->
    case is_atom(Value) of
        true ->
            case Value =:= Atom of
                true -> true;
                false ->
                    {false, [{Value, not_the_atom, T} | CheckStack]}
            end;
        false ->
            {false, [{Value, no_atom, T} | CheckStack]}
    end.

check_tuple(Value, {tuple, Tuple} = T, CheckStack, ParseState) ->
    case is_tuple(Value) of
        true ->
            case erlang:tuple_size(Value) =:= erlang:length(Tuple) of
                true ->
                    case check_tuple_iter(tuple_to_list(Value), Tuple,
                                          CheckStack, ParseState, 1) of
                        {false, ErrStack} ->
                            {false, [{Value, tuple_element_mismatch, T, ErrStack}]};
                        true -> true
                    end;
                _ -> {false, [{Value, not_same_arity, T} | CheckStack]}
            end;
        false ->
            {false, [{Value, no_tuple, T} | CheckStack]}
    end.

check_tuple_iter([], [], _CheckStack, _ParseState, _Count) ->
    true;
check_tuple_iter([Value | Tail], [Type | TypeTail], CheckStack,
                 ParseState, Count) ->
    case inner_check(Value, Type, CheckStack, ParseState) of
        true ->
            check_tuple_iter(Tail, TypeTail, CheckStack, ParseState, Count + 1);
        {false, Stack} ->
            {false, [{Value, tuple_element, Count, Type, Stack}]}
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
            {false, [{Value, no_union_variant_matched,
                      {union, Union}, UnionStack} | CheckStack]}
    end.

-ifdef(with_maps).
check_map(Value, _CheckStack) ->
    _ = maps:size(Value),
    _ = maps:find('$non_existing_key', Value),
    _ = maps:put('$non_existing_key', '$value', Value),
    true.
-else.
check_map(Value, CheckStack) ->
    {false, [{Value, no_map_support} | CheckStack]}.
-endif.

check_fun(Value, {'fun', {product, ParamTypes} = Type, _ResultType},
          CheckStack, _ParseState) ->
    case is_function(Value) of
        false ->
            {false, [{Value, no_function, Type} | CheckStack]};
        true ->
            {arity, Arity} = erlang:fun_info(Value, arity),
            case Arity =:= length(ParamTypes) of
                false ->
                    {false, [{{Value, Arity},
                              fun_with_wrong_arity_for, Type}
                             | CheckStack]};
                true -> true
            end
    end.

-spec log_error(any()) -> ok.
log_error([ErrorReport]) ->
    ErrorIOList = log_error(ErrorReport, ""),
    ct:pal(lists:flatten("TypeCheck:~n" ++ ErrorIOList)),
    ok;
log_error(ErrorReport) ->
    ErrorIOList = log_error(ErrorReport, ""),
    ct:pal(lists:flatten("TypeCheck:~n" ++ ErrorIOList)),
    ok.

log_error([], _Prefix) -> "";
log_error([ErrorReport], Prefix) ->
    log_error(ErrorReport, Prefix ++ "  ");
log_error([H|T], Prefix) ->
    log_error(H, Prefix ++ " |")
        ++ log_error(T, Prefix);
log_error({Val, tuple_element, I, Type, Tree}, Prefix) ->
    io_lib:format(
      Prefix ++ "`-Error:         tuple element ~.0p mismatched~n" ++
      Prefix ++ "  Expected type: ~s~n" ++
      Prefix ++ "  Value:         ~.0p~n" ++
      Prefix ++ "  Because:~n",
      [I, render_type(Type), Val])
      ++ log_error(Tree, Prefix ++ " ");
log_error({Val, list_element, I, Type, Tree}, Prefix) ->
    io_lib:format(
      Prefix ++ "`-Error:         list element ~.0p mismatched~n" ++
      Prefix ++ "  Expected type: ~s~n" ++
      Prefix ++ "  Value:         ~.0p~n" ++
      Prefix ++ "  Because:~n",
      [I, render_type(Type), Val])
      ++ log_error(Tree, Prefix ++ " ");
log_error({Type, defined_as, InnerType, Tree}, Prefix) ->
    io_lib:format(
      Prefix ++ "`-Expected type: ~s~n" ++
      Prefix ++ "  DefinedAs:     ~s~n",
      [render_type(Type), render_type(InnerType)])
      ++ log_error(Tree, Prefix ++ " ");
log_error({Val, Error, Type, Tree}, Prefix) when is_list(Tree) ->
    io_lib:format(
      Prefix ++ "`-Error:         ~.0p~n" ++
      Prefix ++ "  Expected type: ~s~n" ++
      Prefix ++ "  Value:         ~.0p~n" ++
      Prefix ++ "  Because:~n",
      [Error, render_type(Type), Val])
      ++ log_error(Tree, Prefix ++ " ");
log_error({Val, Error, Type}, Prefix) ->
    io_lib:format(
      Prefix ++ "`-Error:         ~.0p~n" ++
      Prefix ++ "  Expected type: ~s~n" ++
      Prefix ++ "  Value:         ~.0p~n",
      [Error, render_type(Type), Val]);
log_error(ErrorReport, Prefix) ->
    io_lib:format(Prefix ++ "`-***~.0p~n", [ErrorReport]).

-spec render_type(any()) -> string().
render_type(Type) ->
    lists:flatten(render_type_(Type)).

render_type_(Basic) when is_atom(Basic) ->
    io_lib:format("~p()", [Basic]);
render_type_({atom, Atom}) ->
    io_lib:format("'~p'", [Atom]);
render_type_({builtin_type, Type}) ->
    io_lib:format("erlang:~p()", [Type]);
render_type_({'fun', {product, _ParamTypes} = P, ResultType}) ->
    "fun(" ++ render_type(P) ++ ") -> " ++
        render_type_(ResultType);
render_type_({integer, Int}) ->
    io_lib:format("~p", [Int]);
render_type_({list, InnerType}) ->
    "[" ++ render_type_(InnerType) ++ "]";
render_type_({nonempty_list, InnerType}) ->
    "nonempty_list: [" ++ render_type_(InnerType) ++ "]";
render_type_({product, TypeList}) when is_list(TypeList) ->
    list_separated_by(TypeList, ", ");
render_type_({range, {integer, Min}, {integer, Max}}) ->
    io_lib:format("~p..~p", [Min, Max]);
render_type_({record, Module, RecordName}) ->
    io_lib:format("~p:#~p{}", [Module, RecordName]);
render_type_({record, FieldList}) when is_list(FieldList) ->
    list_separated_by(FieldList, ", ");
render_type_({tuple, Tuple}) when is_list(Tuple) ->
    "{" ++ list_separated_by(Tuple, ", ") ++ "}";
render_type_({tuple, Tuple}) when is_tuple(Tuple) ->
    "{" ++ render_type_(Tuple) ++ "}";
render_type_({typedef, Module, TypeName}) ->
    io_lib:format("~p:~p()", [Module, TypeName]);
render_type_({typed_record_field, Name, Type}) ->
    io_lib:format("...#{~p :: ~s, ...}", [Name, render_type_(Type)]);
render_type_({union, Union}) ->
    list_separated_by(Union, " | ");
render_type_(Other) ->
    io_lib:format("***~.0p***", [Other]).

list_separated_by([], _Separator) -> "";
list_separated_by([Type], _Separator) ->
    render_type_(Type);
list_separated_by([H|T], Separator) ->
    render_type_(H) ++ Separator ++ list_separated_by(T, Separator).
