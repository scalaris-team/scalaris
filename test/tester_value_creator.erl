%  @copyright 2010-2012 Zuse Institute Berlin

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

-export([create_value/3]).

list_length_min() -> 0.
list_length_max() -> 5.

integer_min() -> 1.
integer_max() -> 5.

%% @doc create a random value of the given type
-spec create_value(type_spec(), non_neg_integer(), tester_parse_state:state()) -> term().
create_value(Type, Size, ParseState) ->
    case tester_global_state:get_value_creator(Type) of
        failed -> create_value_(Type, Size, ParseState);
        Creator -> case custom_value_creator(Creator, Type, Size, ParseState) of
                       failed -> create_value_(Type, Size, ParseState);
                       {value, Value} -> Value
                   end
    end.

-spec create_value_(type_spec(), non_neg_integer(), tester_parse_state:state()) -> term().
create_value_({product, []}, _Size, _ParseState) ->
    [];
create_value_({product, Types}, Size, ParseState) ->
    NewSize = erlang:max(1, (Size - length(Types)) div length(Types)),
    [create_value(Type, NewSize, ParseState) || Type <- Types];
create_value_({tuple, Types}, Size, ParseState) when is_list(Types) ->
    case Types of
        [] ->
            {};
        _ ->
            NewSize = erlang:max(1, (Size - length(Types)) div length(Types)),
            Values = [create_value(Type, NewSize, ParseState) || Type <- Types],
            erlang:list_to_tuple(Values)
    end;
create_value_({tuple, {typedef, tester, test_any}}, Size, ParseState) ->
    Values = create_value({list, {typedef, tester, test_any}}, Size, ParseState),
    erlang:list_to_tuple(Values);
create_value_({list, Type}, Size, ParseState) ->
    ListLength = erlang:min(Size, crypto:rand_uniform(list_length_min(),
                                               list_length_max() + 1)),
    case ListLength of
        0 ->
            [];
        _ ->
            NewSize = erlang:max(1, (Size - ListLength) div ListLength),
            [create_value(Type, NewSize, ParseState) || _ <- lists:seq(1, ListLength)]
    end;
create_value_({nonempty_list, Type}, Size, ParseState) ->
    ListLength =
        erlang:max(1, erlang:min(Size,
                                 crypto:rand_uniform(1, list_length_max() + 1))),
    NewSize = erlang:max(1, (Size - ListLength) div ListLength),
    [create_value(Type, NewSize, ParseState) || _ <- lists:seq(1, ListLength)];
create_value_(nonempty_string, Size, ParseState) ->
    RandStringFun =
        fun() ->
                ListLength0 = crypto:rand_uniform(list_length_min(),
                                                  list_length_max() + 1),
                ListLength = erlang:max(1, erlang:min(Size, ListLength0)),
                Type = {range, {integer, 0}, {integer, 16#10ffff}},
                NewSize = erlang:max(1, (Size - ListLength) div ListLength),
                [create_value(Type, NewSize, ParseState) || _ <- lists:seq(1, ListLength)]
        end,
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_non_empty_strings/1,
      RandStringFun);
create_value_(integer, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_integers/1,
      fun() -> crypto:rand_uniform(integer_min(), integer_max() + 1) end);
% 1..
create_value_(pos_integer, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_pos_integers/1,
      fun() -> crypto:rand_uniform(1, integer_max() + 1) end);
% 0..
create_value_(non_neg_integer, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_non_neg_integers/1,
      fun() -> crypto:rand_uniform(0, integer_max() + 1) end);
create_value_({integer, Value}, _Size, _ParseState) ->
    Value;
create_value_({atom, Value}, _Size, _ParseState) ->
    Value;
create_value_(binary, Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_binaries/1,
      fun() -> list_to_binary(create_value({list, {range, {integer, 0}, {integer, 16#ff}}}, Size, ParseState)) end);
create_value_(bool, _Size, _ParseState) ->
    case crypto:rand_uniform(0, 2) of
        0 -> false;
        1 -> true
    end;
create_value_(nil, _Size, _ParseState) ->
    [];
create_value_(node, _Size, _ParseState) ->
    % @todo
    node();
create_value_(pid, _Size, _ParseState) ->
    % @todo
    self();
create_value_(atom, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_atoms/1,
      fun() -> lists:nth(crypto:rand_uniform(1, 5), [one, two, three, four]) end);
create_value_(float, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_floats/1,
      fun() -> crypto:rand_uniform(-5, 5) * (crypto:rand_uniform(0, 30323) / 30323.0) end);
create_value_({range, {integer, Low}, {integer, High}}, _Size, _ParseState) ->
    crypto:rand_uniform(Low, High + 1);
create_value_({union, Types}, Size, ParseState) ->
    Length = length(Types),
    create_value(lists:nth(crypto:rand_uniform(1, Length + 1), Types),
                 Size, ParseState);
create_value_({record, Module, TypeName}, Size, ParseState) ->
    case tester_parse_state:lookup_type({record, Module, TypeName}, ParseState) of
        {value, RecordType} ->
            create_record_value(TypeName, RecordType, Size, ParseState);
        none ->
            ?ct_fail("error: unknown record type: ~p:~p", [Module, TypeName])
    end;
create_value_({record, _Module, TypeName, FieldTypes}, Size, ParseState) ->
    create_record_value(TypeName, FieldTypes, Size, ParseState);
create_value_({typed_record_field, _Name, Type}, Size, ParseState) ->
    create_value(Type, Size, ParseState);
create_value_({field_type, _Name, Type}, Size, ParseState) ->
    create_value(Type, Size, ParseState);
%%create_value({typedef, tester, test_any}, Size, TypeInfo) ->
    %% @todo
create_value_({typedef, Module, TypeName}, Size, ParseState) ->
    case tester_parse_state:lookup_type({type, Module, TypeName}, ParseState) of
        {value, TypeSpec} ->
            create_value(TypeSpec, Size, ParseState);
        none ->
            ?ct_fail("error: unknown type ~p:~p~n", [Module, TypeName])
    end;
create_value_({builtin_type, array}, Size, ParseState) ->
    L = create_value({list,
                      {tuple,
                       [{typedef, tester, test_any},
                        {typedef, tester, test_any}]}}, Size, ParseState),
    array:from_list(L);
create_value_({builtin_type, gb_tree}, Size, ParseState) ->
    L = create_value({list,
                      {tuple,
                       [{typedef, tester, test_any},
                        {typedef, tester, test_any}]}}, Size, ParseState),
    T = gb_trees:empty(),
    case L of
        [] -> T;
        _ -> lists:foldl(
               fun(X, Acc) ->
                       try gb_trees:insert(element(1, X), element(2,X), Acc) of
                           Y -> Y
                       catch _:_ -> Acc
                       end
               end, T, L)
    end;
create_value_({builtin_type, module}, _Size, _ParseState) ->
    Values = [element(1, X) || X <- code:all_loaded()],
    lists:nth(crypto:rand_uniform(1, length(Values) + 1), Values);
create_value_(Unknown , _Size, _ParseState) ->
    ct:pal("Cannot create type ~.0p~n", [Unknown]),
    throw(function_clause).

%% @doc creates a record value
-spec create_record_value(RecordName :: type_name(),
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
    case crypto:rand_uniform(0, 2) of
        0 -> % take one of the collected values (if possible)
            {Length, Values} = Getter(ParseState),
            case Length of
                0 -> RandValFun();
                _ -> lists:nth(crypto:rand_uniform(1, Length + 1), Values)
            end;
        1 ->
            RandValFun()
    end.

-spec custom_value_creator({module(), atom(), non_neg_integer()}, any(),
                           non_neg_integer(), tester_parse_state:state()) -> failed | {value, any()}.
custom_value_creator({Module, Fun, Arity} = _Creator, Type, Size, ParseState) ->
    % get spec of Creator
    case tester_parse_state:lookup_type({'fun', Module,
                                         Fun, Arity},
                                        ParseState) of
        {value, {union_fun, FunTypes}} ->
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
