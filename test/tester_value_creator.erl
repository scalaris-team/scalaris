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
%% @doc    value creator for tester
%% @end
%% @version $Id$
-module(tester_value_creator).
-author('schuett@zib.de').
-vsn('$Id$').

-include("unittest.hrl").
-include("tester.hrl").

-export([create_value/3, create_value_wo_scalaris/3]).

list_length_min() -> 0.
list_length_max() -> 5.

integer_min() -> 0. %don't change this! (negative values will be created between -max and +max)
integer_max() -> 5.

%% @doc Create a random value of the given type, taking global Scalaris types
%%      into account.
-spec create_value(type_spec(), non_neg_integer(), tester_parse_state:state()) -> term().
create_value(Type, Size, ParseState) ->
    FlatType = case Type of
                   {var_type, [], InnerType} ->
                       InnerType;
                   _ -> Type
               end,
    case tester_value_creator_scalaris:create_value(FlatType, Size, ParseState) of
        failed ->
            create_value_wo_scalaris(FlatType, Size, ParseState);
        {value, Value} ->
            Value
    end.

%% @doc Create a random value of the given type, not taking global Scalaris
%%      types into account at the top level of the given type spec.
-spec create_value_wo_scalaris(type_spec(), non_neg_integer(), tester_parse_state:state()) -> term().
create_value_wo_scalaris(Type, Size, ParseState) ->
    %ct:pal("create_value ~p", [Type]),
    case tester_global_state:get_value_creator(Type) of
        failed ->
            try create_value_(Type, Size, ParseState)
            catch
                throw:{error, Msg} ->
                    NewMsg = tester_type_checker:render_type(Type),
                    throw({error, [NewMsg|Msg]});
                throw:_Term ->
                    NewMsg = tester_type_checker:render_type(Type),
                    throw({error, [NewMsg]})
            end;
        Creator -> case custom_value_creator(Creator, Type, Size, ParseState) of
                       failed -> create_value_(Type, Size, ParseState);
                       {value, Value} -> Value
                   end
    end.

-spec create_value_(type_spec(), non_neg_integer(), tester_parse_state:state()) -> term().


create_value_(atom, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_atoms/1,
      fun() -> lists:nth(randoms:rand_uniform(1, 5), [one, two, three, four]) end);
create_value_({atom, Value}, _Size, _ParseState) ->
    Value;
create_value_({binary, []}, Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_binaries/1,
      fun() -> list_to_binary(create_value({list, {range, {integer, 0}, {integer, 16#ff}}}, Size, ParseState)) end);
create_value_({builtin_type, bitstring}, Size, ParseState) ->
    Binary = create_value_({binary, []}, Size, ParseState),
    RandByte = randoms:rand_uniform(0, 256),
    RandBits = randoms:rand_uniform(0, 8),
    <<Binary/binary,RandByte:RandBits>>;
create_value_(bool, _Size, _ParseState) ->
    case randoms:rand_uniform(0, 2) of
        0 -> false;
        1 -> true
    end;
create_value_({builtin_type, array_array, ValueType}, Size, ParseState) ->
    L = create_value({list, ValueType}, Size, ParseState),
    array:from_list(L);
create_value_({builtin_type, gb_trees_tree, KeyType, ValueType}, Size, ParseState) ->
    L = create_value({list,
                      {tuple,
                       [KeyType, ValueType]}}, Size, ParseState),
    gb_trees:from_orddict(orddict:from_list(L));
create_value_({builtin_type, gb_sets_set, ValueType}, Size, ParseState) ->
    L = create_value({list, ValueType}, Size, ParseState),
    gb_sets:from_list(L);
create_value_({builtin_type, set_set, ValueType}, Size, ParseState) ->
    L = create_value({list, ValueType}, Size, ParseState),
    sets:from_list(L);
create_value_({builtin_type, map}, _Size, ParseState) ->
    create_map(ParseState);
create_value_({builtin_type, maybe_improper_list}, Size, ParseState) ->
    create_value_({list, {typedef, tester, test_any, []}}, Size, ParseState);
create_value_({builtin_type, module}, _Size, _ParseState) ->
    Values = [element(1, X) || X <- code:all_loaded()],
    lists:nth(randoms:rand_uniform(1, length(Values) + 1), Values);
create_value_({field_type, _Name, Type}, Size, ParseState) ->
    create_value(Type, Size, ParseState);
create_value_(float, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_floats/1,
      fun() -> randoms:rand_uniform(-5, 5) * (randoms:rand_uniform(0, 30323) / 30323.0) end);
create_value_(integer, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_integers/1,
      fun() -> randoms:rand_uniform(integer_min(), integer_max() * 2 + 1) - integer_max() end);
create_value_({integer, Value}, _Size, _ParseState) ->
    Value;
create_value_({list, Type}, Size, ParseState) ->
    ListLength = erlang:min(Size, randoms:rand_uniform(list_length_min(),
                                               list_length_max() + 1)),
    case ListLength of
        0 ->
            [];
        _ ->
            NewSize = erlang:max(1, (Size - ListLength) div ListLength),
            [create_value(Type, NewSize, ParseState) || _ <- lists:seq(1, ListLength)]
    end;
create_value_(nil, _Size, _ParseState) ->
    [];
create_value_(node, _Size, _ParseState) ->
    % @todo
    node();
create_value_({nonempty_list, Type}, Size, ParseState) ->
    ListLength =
        erlang:max(1, erlang:min(Size,
                                 randoms:rand_uniform(1, list_length_max() + 1))),
    NewSize = erlang:max(1, (Size - ListLength) div ListLength),
    [create_value(Type, NewSize, ParseState) || _ <- lists:seq(1, ListLength)];
create_value_(nonempty_string, Size, ParseState) ->
    RandStringFun =
        fun() ->
                ListLength0 = randoms:rand_uniform(list_length_min(),
                                                   list_length_max() + 1),
                ListLength = erlang:max(1, erlang:min(Size, ListLength0)),
                Type = {range, {integer, 0}, {integer, 16#10ffff}},
                NewSize = erlang:max(1, (Size - ListLength) div ListLength),
                [create_value(Type, NewSize, ParseState) || _ <- lists:seq(1, ListLength)]
        end,
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_non_empty_strings/1,
      RandStringFun);
% 0..
create_value_(non_neg_integer, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_non_neg_integers/1,
      fun() -> randoms:rand_uniform(0, integer_max() + 1) end);
create_value_(pid, _Size, _ParseState) ->
    case erlang:whereis(tester_pseudo_proc) of
        Pid when is_pid(Pid) -> Pid
    end;
% 1..
create_value_(pos_integer, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_pos_integers/1,
      fun() -> randoms:rand_uniform(1, integer_max() + 1) end);
% ..-1
create_value_(neg_integer, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_neg_integers/1,
      fun() -> -(randoms:rand_uniform(1, integer_max() + 1)) end);
create_value_({product, []}, _Size, _ParseState) ->
    [];
create_value_({product, Types}, Size, ParseState) ->
    NewSize = erlang:max(1, (Size - length(Types)) div length(Types)),
    [create_value(Type, NewSize, ParseState) || Type <- Types];
create_value_({range, {integer, Low}, {integer, High}}, _Size, _ParseState) ->
    randoms:rand_uniform(Low, High + 1);
create_value_({record, Module, TypeName}, Size, ParseState) ->
    case tester_parse_state:lookup_type({record, Module, TypeName}, ParseState) of
        {value, {var_type, [], RecordType}} ->
            create_record_value(TypeName, RecordType, Size, ParseState);
        none ->
            ?ct_fail("error: unknown record type: ~p:~p", [Module, TypeName])
    end;
create_value_({record, _Module, TypeName, FieldTypes}, Size, ParseState) ->
    create_record_value(TypeName, FieldTypes, Size, ParseState);
create_value_({tuple, Types}, Size, ParseState) when is_list(Types) ->
    case Types of
        [] ->
            {};
        _ ->
            NewSize = erlang:max(1, (Size - length(Types)) div length(Types)),
            Values = [create_value(Type, NewSize, ParseState) || Type <- Types],
            erlang:list_to_tuple(Values)
    end;
create_value_({tuple, {typedef, tester, test_any, []}}, Size, ParseState) ->
    Values = create_value({list, {typedef, tester, test_any, []}}, Size, ParseState),
    erlang:list_to_tuple(Values);
%%create_value({typedef, tester, test_any}, Size, TypeInfo) ->
    %% @todo
create_value_({typedef, ets, tid, []}, Size, ParseState) -> % reference since R20
    create_value_(integer, Size, ParseState);
create_value_({typedef, orddict, orddict, []}, Size, ParseState) ->
    KVs = create_value({list,
                        {tuple,
                         [{typedef, tester, test_any, []}, {typedef, tester, test_any, []}]}},
                       Size, ParseState),
    orddict:from_list(KVs);
create_value_({typedef, Module, TypeName, TypeList}, Size, ParseState) ->
    %ct:pal("typedef~n~w~n~w~n", [TypeName, TypeList]),
    %ct:pal("~w", [{type, Module, TypeName, length(TypeList)}]),
    case tester_parse_state:lookup_type({type, Module, TypeName, length(TypeList)}, ParseState) of
        {value, {var_type, [], TypeSpec}} ->
            create_value(TypeSpec, Size, ParseState);
        {value, {var_type, VarList, TypeSpec}} ->
            Subs = tester_variable_substitutions:substitutions_from_list(VarList, TypeList),
            RealTypeSpec = tester_variable_substitutions:substitute(TypeSpec, Subs),
            %ct:pal("type before sub~n~w~n", [TypeSpec]),
            %ct:pal("type after sub~n~w~n", [RealTypeSpec]),
            create_value(RealTypeSpec, Size, ParseState);
        none ->
            ?ct_fail("error: unknown type ~p:~p~n~w~n", [Module, TypeName, TypeList])
    end;
create_value_({typed_record_field, _Name, Type}, Size, ParseState) ->
    create_value(Type, Size, ParseState);
create_value_({union, Types}, Size, ParseState) ->
    Length = length(Types),
    create_value(lists:nth(randoms:rand_uniform(1, Length + 1), Types),
                 Size, ParseState);
create_value_(Unknown , _Size, _ParseState) ->
    ct:pal("Cannot create type (you could register a custom value creator):~n"
           "~.0p~n", [Unknown]),
    throw(function_clause).

%% @doc creates a record value
-spec create_record_value(RecordName :: atom(),
                          {record, Types :: [type_spec()]} | [type_spec()],
                          Size :: non_neg_integer(),
                          ParseState :: tester_parse_state:state()) -> tuple().
create_record_value(RecordName, Types, Size, ParseState) when is_list(Types) ->
    RecordLength = length(Types),
    NewSize = erlang:max(1, (Size - RecordLength) div RecordLength),
    RecordElements = [create_value(Type, NewSize, ParseState) || Type <- Types],
    erlang:list_to_tuple([RecordName | RecordElements]);
create_record_value(RecordName, {record, Types}, Size, ParseState) ->
    create_record_value(RecordName, Types, Size, ParseState).

-spec create_val_50rand_50coll(
        ParseState::tester_parse_state:state(),
        Getter::fun((tester_parse_state:state()) -> {Length::non_neg_integer(), Values::[T]}),
        RandValFun::fun(() -> T)) -> T.
create_val_50rand_50coll(ParseState, Getter, RandValFun) ->
    case randoms:rand_uniform(0, 2) of
        0 -> % take one of the collected values (if possible)
            {Length, Values} = Getter(ParseState),
            case Length of
                0 -> RandValFun();
                _ -> lists:nth(randoms:rand_uniform(1, Length + 1), Values)
            end;
        1 ->
            RandValFun()
    end.

-ifdef(with_maps).
-spec create_map(ParseState::tester_parse_state:state()) -> map().
create_map(ParseState) ->
    L = create_value({list, {tuple,
                             [{typedef, tester, test_any, []},
                              {typedef, tester, test_any, []}]}}, 0, ParseState),
    maps:from_list(L).
-else.
-spec create_map(ParseState::tester_parse_state:state()) -> no_return().
create_map(_ParseState) ->
    erlang:error('maps_not_available').
-endif.

-spec custom_value_creator({module(), atom(), non_neg_integer()}, any(),
                           non_neg_integer(), tester_parse_state:state()) -> failed | {value, any()}.
custom_value_creator({Module, Fun, Arity} = _Creator, Type, Size, ParseState) ->
    % get spec of Creator
    case tester_parse_state:lookup_type({'fun', Module,
                                         Fun, Arity},
                                        ParseState) of
        {value, {var_type, [], {union_fun, FunTypes}}} ->
            {'fun', ArgType, _ResultType} = util:randomelem(FunTypes),
            Args = try {value, tester_value_creator:create_value(ArgType, Size, ParseState)}
                   catch
                       Error:Reason ->
                           ct:pal("could not generate input for custom value creator ~p:~p/~p: ~p:~p",
                                  [Module, Fun, Arity, Error, Reason]),
                           failed
                   end,
            case Args of
                failed ->
                    failed;
                {value, Args2} ->
                    try {value, erlang:apply(Module, Fun, Args2)}
                    catch
                        Error2:Reason2 ->
                            ct:pal("exception in custom value creator ~p:~p/~p(~p): ~p:~p",
                                   [Module, Fun, Arity, Args, Error2, Reason2]),
                            failed
                    end
            end;
        none ->
            ct:pal("could not call custom value creator (~p:~p/~p) for ~p",
                   [Module, Fun, Arity, Type]),
            failed
    end.
