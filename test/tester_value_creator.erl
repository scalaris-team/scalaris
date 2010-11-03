%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
create_value({product, Types}, Size, ParseState) ->
    NewSize = erlang:max(1, (Size - length(Types)) div length(Types)),
    [create_value(Type, NewSize, ParseState) || Type <- Types];
create_value({tuple, Types}, Size, ParseState) when is_list(Types) ->
    case Types of
        [] ->
            {};
        _ ->
            NewSize = erlang:max(1, (Size - length(Types)) div length(Types)),
            Values = [create_value(Type, NewSize, ParseState) || Type <- Types],
            erlang:list_to_tuple(Values)
    end;
create_value({tuple, {typedef, tester, test_any}}, Size, ParseState) ->
    Values = create_value({list, {typedef, tester, test_any}}, Size, ParseState),
    erlang:list_to_tuple(Values);
create_value({list, Type}, Size, ParseState) ->
    ListLength = erlang:min(Size, crypto:rand_uniform(list_length_min(),
                                               list_length_max() + 1)),
    case ListLength of
        0 ->
            [];
        _ ->
            NewSize = erlang:max(1, (Size - ListLength) div ListLength),
            [create_value(Type, NewSize, ParseState) || _ <- lists:seq(1, ListLength)]
    end;
create_value({nonempty_list, Type}, Size, ParseState) ->
    ListLength =
        erlang:max(1, erlang:min(Size,
                                 crypto:rand_uniform(1, list_length_max() + 1))),
    NewSize = erlang:max(1, (Size - ListLength) div ListLength),
    [create_value(Type, NewSize, ParseState) || _ <- lists:seq(1, ListLength)];
create_value(nonempty_string, Size, ParseState) ->
    ListLength = erlang:max(1, erlang:min(Size,
                                          crypto:rand_uniform(list_length_min(),
                                                              list_length_max() + 1))),
    Type = {range, {integer, 0}, {integer, 16#10ffff}},
    NewSize = erlang:max(1, (Size - ListLength) div ListLength),
    RandString = [create_value(Type, NewSize, ParseState) || _ <- lists:seq(1, ListLength)],
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_non_empty_strings/1,
      RandString);
create_value(integer, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_integers/1,
      crypto:rand_uniform(integer_min(), integer_max() + 1));
% 1..
create_value(pos_integer, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_pos_integers/1,
      crypto:rand_uniform(1, integer_max() + 1));
% 0..
create_value(non_neg_integer, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_non_neg_integers/1,
      crypto:rand_uniform(0, integer_max() + 1));
create_value({integer, Value}, _Size, _ParseState) ->
    Value;
create_value({atom, Value}, _Size, _ParseState) ->
    Value;
create_value(binary, _Size, ParseState) ->
    {Length, Binaries} = tester_parse_state:get_binaries(ParseState),
    case Length of
        0 -> ?ct_fail("error: cannot create binaries~n", []);
        _ -> lists:nth(crypto:rand_uniform(1, Length + 1), Binaries)
    end;
create_value(bool, _Size, _ParseState) ->
    case crypto:rand_uniform(0, 2) of
        0 -> false;
        1 -> true
    end;
create_value(nil, _Size, _ParseState) ->
    [];
create_value(node, _Size, _ParseState) ->
    % @todo
    node();
create_value(pid, _Size, _ParseState) ->
    % @todo
    self();
create_value(atom, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_atoms/1,
      lists:nth(crypto:rand_uniform(1, 5), [one, two, three, four]));
create_value(float, _Size, ParseState) ->
    create_val_50rand_50coll(
      ParseState, fun tester_parse_state:get_floats/1,
      crypto:rand_uniform(-5, 5) * (crypto:rand_uniform(0, 30323) / 30323.0));
create_value({range, {integer, Low}, {integer, High}}, _Size, _ParseState) ->
    crypto:rand_uniform(Low, High + 1);
create_value({union, Types}, Size, ParseState) ->
    Length = length(Types),
    create_value(lists:nth(crypto:rand_uniform(1, Length + 1), Types),
                 Size, ParseState);
create_value({record, Module, TypeName}, Size, ParseState) ->
    case tester_parse_state:lookup_type({record, Module, TypeName}, ParseState) of
        {value, RecordType} ->
            create_record_value(TypeName, RecordType, Size, ParseState);
        none ->
            ?ct_fail("error: unknown record type: ~p:~p", [Module, TypeName])
    end;
create_value({record, _Module, TypeName, FieldTypes}, Size, ParseState) ->
    create_record_value(TypeName, FieldTypes, Size, ParseState);
create_value({typed_record_field, _Name, Type}, Size, ParseState) ->
    create_value(Type, Size, ParseState);
create_value({field_type, _Name, Type}, Size, ParseState) ->
    create_value(Type, Size, ParseState);
%%create_value({typedef, tester, test_any}, Size, TypeInfo) ->
    %% @todo
create_value({typedef, Module, TypeName}, Size, ParseState) ->
    case tester_parse_state:lookup_type({type, Module, TypeName}, ParseState) of
        {value, TypeSpec} ->
            create_value(TypeSpec, Size, ParseState);
        none ->
            ?ct_fail("error: unknown type ~p:~p~n", [Module, TypeName])
    end.

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
        RandVal::T) -> T.
create_val_50rand_50coll(ParseState, Getter, RandVal) ->
    case crypto:rand_uniform(0, 2) of
        0 -> % take one of the collected values (if possible)
            {Length, Values} = Getter(ParseState),
            case Length of
                0 -> RandVal;
                _ -> lists:nth(crypto:rand_uniform(1, Length + 1), Values)
            end;
        1 ->
            RandVal
    end.
