%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    tester.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    test generator
%%% @end
%%% Created :  30 March 2010 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(tester).

-author('schuett@zib.de').
-vsn('$Id$').

-export([test/4]).

% for unit tests
-export([create_value/3]).

-include_lib("tester.hrl").
-include_lib("unittest.hrl").

list_length_min() -> 0.
list_length_max() -> 5.

integer_min() -> 1.
integer_max() -> 5.

-spec test/4 :: (module(), atom(), non_neg_integer(), non_neg_integer()) -> ok.
test(Module, Func, Arity, Iterations) ->
    EmptyParseState = tester_parse_state:new_parse_state(),
    ParseState = collect_fun_info(Module, Func, Arity,
                                  EmptyParseState),
    run(Module, Func, Arity, Iterations, ParseState),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% collect necessary type information
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec collect_fun_info/4 :: (module(), atom(), non_neg_integer(), #parse_state{}) -> #parse_state{}.
collect_fun_info(Module, Func, Arity, ParseState) ->
    ParseState2 = case tester_parse_state:lookup_type({'fun', Module, Func, Arity}, ParseState) of
                    {value, _} ->
                        ParseState;
                    none ->
                        {ok, {Module, [{abstract_code, {_AbstVersion, AbstractCode}}]}}
                            = beam_lib:chunks(code:where_is_file(atom_to_list(Module) ++ ".beam"), [abstract_code]),
                        lists:foldl(fun (Chunk, InnerParseState) ->
                                            parse_chunk(Chunk, Module, InnerParseState)
                                    end, ParseState, AbstractCode)
                end,
    ParseState3 = case tester_parse_state:get_unknown_types(ParseState2) of
        [] ->
            ParseState2;
        UnknownTypes ->
            collect_unknown_type_infos(UnknownTypes, ParseState2, 100)
    end,
    case tester_parse_state:lookup_type({'fun', Module, Func, Arity}, ParseState3) of
        {value, _} ->
            ParseState3;
        none ->
            ?ct_fail("unknown function ~p:~p/~p~n", [Module, Func, Arity])
    end.

-spec collect_unknown_type_infos/3 :: (list(type_name()), #parse_state{}, non_neg_integer()) -> #parse_state{}.
collect_unknown_type_infos(UnknownTypes, ParseState, 0) ->
    ct:pal("warning still looking for unknown types: ~p~n", [tester_parse_state:get_unknown_types(ParseState)]),
    collect_unknown_type_infos(UnknownTypes, ParseState, 100);
collect_unknown_type_infos(UnknownTypes, ParseState, Counter) ->
    ParseState2 = tester_parse_state:reset_unknown_types(ParseState),
    ParseState3 = lists:foldl(fun ({type, Module, TypeName}, InnerParseState) ->
                                        collect_type_info(Module, TypeName, InnerParseState)
                                end, ParseState2, UnknownTypes),
    case tester_parse_state:get_unknown_types(ParseState3) of
        [] ->
            ParseState3;
        NewUnknownTypes ->
            collect_unknown_type_infos(NewUnknownTypes, ParseState3, Counter - 1)
    end.

-spec collect_type_info/3 :: (module(), atom(), #parse_state{}) -> #parse_state{}.
collect_type_info(Module, Type, ParseState) ->
    case tester_parse_state:is_known_type(Module, Type, ParseState) of
        true ->
            ParseState;
        false ->
            {ok, {Module, [{abstract_code, {_AbstVersion, AbstractCode}}]}}
                = beam_lib:chunks(code:where_is_file(atom_to_list(Module) ++ ".beam"), [abstract_code]),
            lists:foldl(fun (Chunk, InnerParseState) ->
                                parse_chunk(Chunk, Module, InnerParseState)
                        end, ParseState, AbstractCode)
    end.


-spec parse_chunk/3 :: (any(), module(), #parse_state{}) -> #parse_state{}.
parse_chunk({attribute, _Line, type, {{record, TypeName}, ATypeSpec, _List}}, Module, ParseState) ->
    {TheTypeSpec, NewParseState} = parse_type(ATypeSpec, Module, ParseState),
    tester_parse_state:add_type_spec({type, Module, TypeName}, TheTypeSpec, NewParseState);
parse_chunk({attribute, _Line, type, {TypeName, ATypeSpec, _List}}, Module, ParseState) ->
    {TheTypeSpec, NewParseState} = parse_type(ATypeSpec, Module, ParseState),
    tester_parse_state:add_type_spec({type, Module, TypeName}, TheTypeSpec, NewParseState);
parse_chunk({attribute, _Line, 'spec', {{FunName, FunArity}, [AFunSpec]}}, Module, ParseState) ->
    {TheFunSpec, NewParseState} = parse_type(AFunSpec, Module, ParseState),
    tester_parse_state:add_type_spec({'fun', Module, FunName, FunArity}, TheFunSpec, NewParseState);
parse_chunk({attribute, _Line, record, {TypeName, TypeList}}, Module, ParseState) ->
    {TheTypeSpec, NewParseState} = parse_type(TypeList, Module, ParseState),
    tester_parse_state:add_type_spec({type, Module, TypeName}, TheTypeSpec, NewParseState);
parse_chunk({attribute, _Line, _AttributeName, _AttributeValue}, _Module, ParseState) ->
    ParseState;
parse_chunk({function, _Line, _FunName, _FunArity, FunCode}, _Module, ParseState) ->
    tester_value_collector:parse_expression(FunCode, ParseState);
parse_chunk({eof, _Line}, _Module, ParseState) ->
    ParseState.

-spec parse_type/3 :: (any(), module(), #parse_state{}) -> {type_spec() , #parse_state{}}.
parse_type({type, _Line, 'fun', [Arg, Result]}, Module, ParseState) ->
    {ArgType, ParseState2} = parse_type(Arg, Module, ParseState),
    {ResultType, ParseState3} = parse_type(Result, Module, ParseState2),
    {{'fun', ArgType, ResultType}, ParseState3};
parse_type({type, _Line, product, Types}, Module, ParseState) ->
    {TypeList, ParseState2} = parse_type_list(Types, Module, ParseState),
    {{product, TypeList}, ParseState2};
parse_type({type, _Line, tuple, any}, _Module, ParseState) ->
    {{tuple, {typedef, tester, test_any}}, ParseState};
parse_type({type, _Line, tuple, Types}, Module, ParseState) ->
    {TypeList, ParseState2} = parse_type_list(Types, Module, ParseState),
    {{tuple, TypeList}, ParseState2};
parse_type({type, _Line, list, [Type]}, Module, ParseState) ->
    {ListType, ParseState2} = parse_type(Type, Module, ParseState),
    {{list, ListType}, ParseState2};
parse_type({type, _Line, nonempty_list, [Type]}, Module, ParseState) ->
    {ListType, ParseState2} = parse_type(Type, Module, ParseState),
    {{nonempty_list, ListType}, ParseState2};
parse_type({type, _Line, list, []}, _Module, ParseState) ->
    {{list, {typedef, tester, test_any}}, ParseState};
parse_type({type, _Line, range, [Begin, End]}, Module, ParseState) ->
    {BeginType, ParseState2} = parse_type(Begin, Module, ParseState),
    {EndType, ParseState3} = parse_type(End, Module, ParseState2),
    {{range, BeginType, EndType}, ParseState3};
parse_type({type, _Line, union, Types}, Module, ParseState) ->
    {TypeList, ParseState2} = parse_type_list(Types, Module, ParseState),
    {{union, TypeList}, ParseState2};
parse_type({type, _Line, integer, []}, _Module, ParseState) ->
    {integer, ParseState};
parse_type({type, _Line, pos_integer, []}, _Module, ParseState) ->
    {pos_integer, ParseState};
parse_type({type, _Line, non_neg_integer, []}, _Module, ParseState) ->
    {non_neg_integer, ParseState};
parse_type({type, _Line, bool, []}, _Module, ParseState) ->
    {bool, ParseState};
parse_type({type, _Line, char, []}, _Module, ParseState) ->
    {{range, {integer, 0}, {integer, 16#10ffff}}, ParseState};
parse_type({type, _Line, string, []}, _Module, ParseState) ->
    {{list, {range, {integer, 0}, {integer, 16#10ffff}}}, ParseState};
parse_type({type, _Line, nonempty_string, []}, _Module, ParseState) ->
    {nonempty_string, ParseState};
parse_type({type, _Line, number, []}, _Module, ParseState) ->
    {{union, [integer, float]}, ParseState};
parse_type({type, _Line, boolean, []}, _Module, ParseState) ->
    {bool, ParseState};
parse_type({type, _Line, any, []}, _Module, ParseState) ->
    {{typedef, tester, test_any}, ParseState};
parse_type({type, _Line, atom, []}, _Module, ParseState) ->
    {atom, ParseState};
parse_type({type, _Line, binary, []}, _Module, ParseState) ->
    {binary, ParseState};
parse_type({type, _Line, pid, []}, _Module, ParseState) ->
    {pid, ParseState};
parse_type({type, _Line, tid, []}, _Module, ParseState) ->
    {tid, ParseState};
parse_type({type, _Line, port, []}, _Module, ParseState) ->
    {port, ParseState};
parse_type({type, _Line, float, []}, _Module, ParseState) ->
    {float, ParseState};
parse_type({type, _Line, iolist, []}, _Module, ParseState) ->
    {iolist, ParseState};
parse_type({type, _Line, nil, []}, _Module, ParseState) ->
    {nil, ParseState};
parse_type({type, _Line, node, []}, _Module, ParseState) ->
    {node, ParseState};
parse_type({type, _Line, none, []}, _Module, ParseState) ->
    {none, ParseState};
parse_type({type, _Line, reference, []}, _Module, ParseState) ->
    {reference, ParseState};
parse_type({type, _Line, term, []}, _Module, ParseState) ->
    {{typedef, tester, test_any}, ParseState};
parse_type({ann_type, _Line, [{var, _Line2, _Varname}, Type]}, Module, ParseState) ->
    parse_type(Type, Module, ParseState);
parse_type({atom, _Line, Atom}, _Module, ParseState) ->
    {{atom, Atom}, ParseState};
parse_type({integer, _Line, Value}, _Module, ParseState) ->
    {{integer, Value}, ParseState};
parse_type({type, _Line, dict, []}, _Module, ParseState) ->
    {{builtin_type, dict}, ParseState};
parse_type({type, _Line, gb_set, []}, _Module, ParseState) ->
    {{builtin_type, gb_set}, ParseState};
parse_type({type, _Line, gb_tree, []}, _Module, ParseState) ->
    {{builtin_type, gb_tree}, ParseState};
parse_type({type, _Line, module, []}, _Module, ParseState) ->
    {{builtin_type, module}, ParseState};
parse_type({remote_type, _Line, [{atom, _Line2, TypeModule}, {atom, _line3, TypeName}, []]}, _Module, ParseState) ->
    case tester_parse_state:is_known_type(TypeModule, TypeName, ParseState) of
        true ->
            {{typedef, TypeModule, TypeName}, ParseState};
        false ->
            {{typedef, TypeModule, TypeName}, tester_parse_state:add_unknown_type(TypeModule, TypeName, ParseState)}
    end;
parse_type({type, _Line, TypeName, []}, Module, ParseState) ->
    %ct:pal("type1 ~p:~p", [Module, TypeName]),
    case tester_parse_state:is_known_type(Module, TypeName, ParseState) of
        true ->
            {{typedef, Module, TypeName}, ParseState};
        false ->
            {{typedef, Module, TypeName}, tester_parse_state:add_unknown_type(Module, TypeName, ParseState)}
    end;
parse_type({type, _Line, record, [{atom, _Line2, TypeName}]}, Module, ParseState) ->
    {{record, Module, TypeName}, ParseState};
parse_type({typed_record_field, {record_field, _Line, {atom, _Line2, FieldName}}, Field}, Module, ParseState) ->
    {FieldType, ParseState2} = parse_type(Field, Module, ParseState),
    {{typed_record_field, FieldName, FieldType}, ParseState2};
parse_type({typed_record_field, {record_field, _Line, {atom, _Line2, FieldName}, _Default}, Field}, Module, ParseState) ->
    {FieldType, ParseState2} = parse_type(Field, Module, ParseState),
    {{typed_record_field, FieldName, FieldType}, ParseState2};
parse_type({record_field, _Line, {atom, _Line2, FieldName}}, _Module, ParseState) ->
    {{untyped_record_field, FieldName}, ParseState};
parse_type({record_field, _Line, {atom, _Line2, FieldName}, _Default}, _Module, ParseState) ->
    {{untyped_record_field, FieldName}, ParseState};
parse_type(TypeSpecs, Module, ParseState) when is_list(TypeSpecs) ->
    case hd(TypeSpecs) of
        {typed_record_field, _, _} ->
            {RecordType, ParseState2} = parse_type_list(TypeSpecs, Module, ParseState),
            {{record, RecordType}, ParseState2};
        {record_field, _, _} ->
            {RecordType, ParseState2} = parse_type_list(TypeSpecs, Module, ParseState),
            {{record, RecordType}, ParseState2};
        {record_field, _, _, _} ->
            {RecordType, ParseState2} = parse_type_list(TypeSpecs, Module, ParseState),
            {{record, RecordType}, ParseState2};
        _ ->
            io:format("potentially unknown type2: ~p~n", [TypeSpecs]),
            unknown
    end;
parse_type(TypeSpec, _Module, ParseState) ->
    ct:pal("unknown type ~p", [TypeSpec]),
    {unkown, ParseState}.

-spec parse_type_list/3 :: (list(type_spec()), module(), #parse_state{}) -> {list(type_spec()), #parse_state{}}.
parse_type_list(List, Module, ParseState) ->
    case List of
        [] ->
            {[], ParseState};
        [Head | Tail] ->
            {Type, ParseState2} = parse_type(Head, Module, ParseState),
            {TypeList, ParseState3} = parse_type_list(Tail, Module, ParseState2),
            {[Type | TypeList], ParseState3}
    end.

-spec run/5 :: (module(), atom(), non_neg_integer(), non_neg_integer(), #parse_state{}) -> ok.
run(Module, Func, Arity, Iterations, ParseState) ->
    {value, FunType} = tester_parse_state:lookup_type({'fun', Module, Func, Arity}, ParseState),
    run_helper(Module, Func, Arity, Iterations, FunType, ParseState).

-spec run_helper/6 :: (module(), atom(), non_neg_integer(), non_neg_integer(),
                       {'fun', type_spec(), type_spec()}, #parse_state{}) -> ok.
run_helper(_Module, _Func, _Arity, 0, _FunType, _TypeInfos) ->
    ok;
run_helper(Module, Func, 0, Iterations, {'fun', _ArgType, _ResultType} = FunType, TypeInfos) ->
    try erlang:apply(Module, Func, []) of
        true ->
            run_helper(Module, Func, 0, Iterations - 1, FunType, TypeInfos);
        X ->
            ?ct_fail("error ~p:~p(~p) failed with ~p~n", [Module, Func, [], X])
    catch
        throw:Term -> ?ct_fail("exception (throw) in ~p:~p(~p): ~p~n", [Module, Func, [], {exception, Term}]);
        % special handling for exits that come from a ct:fail() call:
        exit:{test_case_failed, _} = Reason -> exit(Reason);
        exit:Reason -> ?ct_fail("exception (exit) in ~p:~p(~p): ~p~n", [Module, Func, [], {exception, Reason}]);
        error:Reason -> ?ct_fail("exception (error) in ~p:~p(~p): ~p~n", [Module, Func, [], {exception, {Reason, erlang:get_stacktrace()}}])
    end;
run_helper(Module, Func, Arity, Iterations, {'fun', ArgType, _ResultType} = FunType, ParseState) ->
    Size = 30,
    Args = create_value(ArgType, Size, ParseState),
    try erlang:apply(Module, Func, Args) of
        true ->
            run_helper(Module, Func, Arity, Iterations - 1, FunType, ParseState);
        X ->
            ?ct_fail("error ~p:~p(~p) failed with ~p~n", [Module, Func, Args, X])
    catch
        throw:Term -> ?ct_fail("exception (throw) in ~p:~p(~p): ~p~n", [Module, Func, Args, {exception, Term}]);
        % special handling for exits that come from a ct:fail() call:
        exit:{test_case_failed, _} = Reason -> exit(Reason);
        exit:Reason -> ?ct_fail("exception (exit) in ~p:~p(~p): ~p~n", [Module, Func, Args, {exception, Reason}]);
        error:Reason -> ?ct_fail("exception (error) in ~p:~p(~p): ~p~n", [Module, Func, Args, {exception, {Reason, erlang:get_stacktrace()}}])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% create a random value of the given type
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc create a random value of the given type
-spec(create_value/3 :: (type_spec(), non_neg_integer(), #parse_state{}) -> term()).
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
    ListLength = erlang:max(1, erlang:min(Size, crypto:rand_uniform(list_length_min(),
                                                                    list_length_max() + 1))),
    Type = {range, {integer, 0}, {integer, 16#10ffff}},
    NewSize = erlang:max(1, (Size - ListLength) div ListLength),
    [create_value(Type, NewSize, ParseState) || _ <- lists:seq(1, ListLength)];
create_value(integer, _Size, _ParseState) ->
    crypto:rand_uniform(integer_min(), integer_max() + 1);
create_value(pos_integer, _Size, ParseState) ->
    case crypto:rand_uniform(0, 2) of
        0 ->
            % take one of the collected integers
            Integers = tester_parse_state:get_integers(ParseState),
            case length(Integers) of
                0 ->
                    crypto:rand_uniform(1, integer_max() + 1);
                Length ->
                    lists:nth(crypto:rand_uniform(1 , Length + 1), Integers)
            end;
        1 ->
            crypto:rand_uniform(1, integer_max() + 1)
    end;
create_value(non_neg_integer, _Size, _ParseState) ->
    crypto:rand_uniform(0, integer_max() + 1);
create_value({integer, Value}, _Size, _ParseState) ->
    Value;
create_value({atom, Value}, _Size, _ParseState) ->
    Value;
create_value(bool, _Size, _ParseState) ->
    case crypto:rand_uniform(0, 2) of
        0 ->
            false;
        1 ->
            true
    end;
create_value(nil, _Size, _ParseState) ->
    [];
create_value(node, _Size, _ParseState) ->
    % @todo
    node();
create_value(pid, _Size, _ParseState) ->
    % @todo
    self();
create_value(atom, _Size, _ParseState) ->
    Atoms = [one, two, three, four],
    lists:nth(crypto:rand_uniform(1, length(Atoms) + 1), Atoms);
create_value(float, _Size, _ParseState) ->
    crypto:rand_uniform(-5, 5) * (crypto:rand_uniform(0, 30323) / 30323);
create_value({range, {integer, Low}, {integer, High}}, _Size, _ParseState) ->
    crypto:rand_uniform(Low, High + 1);
create_value({union, Types}, Size, ParseState) ->
    Length = length(Types),
    create_value(lists:nth(crypto:rand_uniform(1, Length + 1), Types),
                 Size, ParseState);
create_value({record, Module, TypeName}, Size, ParseState) ->
    case tester_parse_state:lookup_type({type, Module, TypeName}, ParseState) of
        {value, RecordType} ->
            create_record_value(TypeName, RecordType, Size, ParseState);
        none ->
            ?ct_fail("error: unknown record type: ~p:~p", [Module, TypeName])
    end;
create_value({typed_record_field, _Name, Type}, Size, ParseState) ->
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
-spec create_record_value/4 :: (RecordName :: type_name(),
                                {record, Types :: [type_spec()]},
                                Size :: non_neg_integer(),
                                ParseState :: #parse_state{}) -> tuple().
create_record_value(RecordName, {record, Types}, Size, ParseState) ->
    RecordLength = length(Types),
    NewSize = erlang:max(1, (Size - RecordLength) div RecordLength),
    RecordElements = [create_value(Type, NewSize, ParseState) || Type <- Types],
    erlang:list_to_tuple([RecordName | RecordElements]).
