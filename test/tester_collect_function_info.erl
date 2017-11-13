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
%% @doc    collection type information about a function
%% @end
%% @version $Id$
-module(tester_collect_function_info).
-author('schuett@zib.de').
-vsn('$Id$').

-export([collect_fun_info/4]).
-export([unittest_collect_module_info/2]).

-include("tester.hrl").
-include("unittest.hrl").

-spec unittest_collect_module_info(module(), tester_parse_state:state()) ->
                                          tester_parse_state:state().
unittest_collect_module_info(Module, ParseState) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    erlang:put(module, Module),
    ModuleFile = code:where_is_file(atom_to_list(Module) ++ ".beam"),
    case beam_lib:chunks(ModuleFile, [abstract_code]) of
        {ok, {Module, [{abstract_code, {_AbstVersion, AbstractCode}}]}} ->
            lists:foldl(fun(Chunk, InnerParseState) ->
                                parse_chunk_log(Chunk, Module, InnerParseState)
                        end, ParseState, AbstractCode);
        {ok, {Module, [{abstract_code, no_abstract_code}]}} ->
            ?ct_fail("the module ~s was not compiled with debug_info~n~p",
                     [Module, util:get_stacktrace()])
    end.

-spec collect_fun_info(module(), atom(), non_neg_integer(),
                       tester_parse_state:state()) -> tester_parse_state:state().
collect_fun_info(Module, Func, Arity, ParseState) ->
    erlang:put(module, Module),
    ParseState2 =
        case tester_parse_state:lookup_type({'fun', Module, Func, Arity}, ParseState) of
            {value, _} -> ParseState;
            none ->
                ModuleFile = code:where_is_file(atom_to_list(Module) ++ ".beam"),
                {ok, {Module, [{abstract_code, {_AbstVersion, AbstractCode}}]}}
                    = beam_lib:chunks(ModuleFile, [abstract_code]),
                lists:foldl(fun(Chunk, InnerParseState) ->
                                    parse_chunk_log(Chunk, Module, InnerParseState)
                            end, ParseState, AbstractCode)
        end,
    ParseState3 = case tester_parse_state:has_unknown_types(ParseState2) of
                      false -> ParseState2;
                      true  -> collect_unknown_type_infos(ParseState2, [])
                  end,
    case tester_parse_state:lookup_type({'fun', Module, Func, Arity}, ParseState3) of
        {value, _} -> tester_parse_state:finalize(ParseState3);
        none -> ?ct_fail("no '-spec' definition for function ~p:~p/~p found by tester~n", [Module, Func, Arity])
    end.

-spec collect_unknown_type_infos(tester_parse_state:state(), list()) ->
    tester_parse_state:state().
collect_unknown_type_infos(ParseState, OldUnknownTypes) ->
    {_, UnknownTypes} = tester_parse_state:get_unknown_types(ParseState),
    %ct:pal("unknown types: ~p~n", [UnknownTypes]),
    case OldUnknownTypes =:= UnknownTypes of
        true ->
            ct:pal("never found the following types: ~p~n~n", [UnknownTypes]),
            ?ct_fail("never found the following types: ~p~n~n", [UnknownTypes]),
            error;
        false ->
            ParseState2 = tester_parse_state:reset_unknown_types(ParseState),
            ParseState3 = lists:foldl(fun({type, Module, TypeName, Arity}, InnerParseState) ->
                                              collect_type_info(Module, TypeName, Arity,
                                                                InnerParseState)
                                      end, ParseState2, UnknownTypes),
            case tester_parse_state:has_unknown_types(ParseState3) of
                false -> ParseState3;
                true  -> collect_unknown_type_infos(ParseState3, UnknownTypes)
            end
    end.

-spec collect_type_info(module(), atom(), arity(), tester_parse_state:state()) ->
    tester_parse_state:state().
collect_type_info(Module, Type, Arity, ParseState) ->
    erlang:put(module, Module),
    case tester_parse_state:is_known_type(Module, Type, Arity, ParseState) of
        true ->
            ParseState;
        false ->
            case code:where_is_file(atom_to_list(Module) ++ ".beam") of
                non_existing ->
                    ct:pal("Error: File \"~w\" not found while trying to collect type info for ~w:~w/~w.",
                           [Module, Module, Type, Arity]),
                    ?ct_fail("File \"~w\" not found while trying to collect type info for ~w:~w/~w.",
                           [Module, Module, Type, Arity]),
                    error;
                FileName ->
                    {ok, {Module, [{abstract_code, {_AbstVersion, AbstractCode}}]}}
                    = beam_lib:chunks(FileName, [abstract_code]),
                    lists:foldl(fun (Chunk, InnerParseState) ->
                                        parse_chunk_log(Chunk, Module, InnerParseState)
                                end, ParseState, AbstractCode)
            end
    end.

-spec parse_chunk_log(any(), module(), tester_parse_state:state()) ->
                             tester_parse_state:state().
parse_chunk_log(Type, Module, State) ->
    try
        parse_chunk(Type, Module, State)
    catch
        subst_error ->
            ct:pal("~p:~p: failed to substitute", [Module, Type]),
            exit(foobar);
        parse_error ->
            ct:pal("~p:~p: failed to parse chunk", [Module, Type]),
            exit(foobar)%;
%        error:Reason ->
%            ct:pal("~p:~p: failed to parse chunk (error:~p)", [Module, Type, Reason]),
%            exit(foobar)
    end.

-spec parse_chunk(any(), module(), tester_parse_state:state()) ->
                         tester_parse_state:state().
parse_chunk({attribute, _Line, type, {{record, TypeName}, ATypeSpec, List}},
            Module, ParseState) ->
    {TheTypeSpec, NewParseState} = parse_type_log(ATypeSpec, Module, ParseState,
                                                  {record_type_attribute, TypeName}),
    tester_parse_state:add_type_spec({record, Module, TypeName}, TheTypeSpec, List,
                                     NewParseState);
parse_chunk({attribute, _Line, type, {TypeName, ATypeSpec, List}},
            Module, ParseState) ->
    {TheTypeSpec, NewParseState} = parse_type_log(ATypeSpec, Module, ParseState,
                                                  {type_attribute, TypeName}),
    tester_parse_state:add_type_spec({type, Module, TypeName, length(List)}, TheTypeSpec, List,
                                     NewParseState);
parse_chunk({attribute, _Line, opaque, {TypeName, ATypeSpec, List}},
            Module, ParseState) ->
    {TheTypeSpec, NewParseState} = parse_type_log(ATypeSpec, Module, ParseState,
                                                  {opaque_type_attribute, TypeName}),
    tester_parse_state:add_type_spec({type, Module, TypeName, length(List)}, TheTypeSpec, List,
                                     NewParseState);
parse_chunk({attribute, _Line, 'spec', {{FunName, FunArity}, AFunSpec}},
            Module, ParseState) ->
    FunSpec = [case TheFunSpec of
                  {type, _,bounded_fun, [_TypeFun, ConstraintType]} ->
                      try
                          Substitutions = parse_constraints(ConstraintType, gb_trees:empty()),
                          tester_variable_substitutions:substitute(TheFunSpec, Substitutions)
                      catch
                          {subst_error, Description} ->
                              ct:pal("substitution error ~w in ~w:~w ~w", [Description, Module, FunName, AFunSpec]),
                              throw(subst_error);
                          {parse_error, Description} ->
                              ct:pal("parse error ~w in ~w:~w ~w", [Description, Module, FunName, AFunSpec]),
                              throw(parse_error)
                      end;
                  _ ->
                      TheFunSpec
              end || TheFunSpec <- AFunSpec],
    {CleanFunSpec, NewParseState} = parse_type_log({union_fun, FunSpec}, Module, ParseState,
                                                   {fun_spec, FunName}),
    tester_parse_state:add_type_spec({'fun', Module, FunName, FunArity},
                                     CleanFunSpec, [], NewParseState);
parse_chunk({attribute, _Line, record, {TypeName, TypeList}}, Module, ParseState) ->
    {TheTypeSpec, NewParseState} = parse_type_log(TypeList, Module, ParseState,
                                                  {record_attribute, TypeName}),
    tester_parse_state:add_type_spec({record, Module, TypeName}, TheTypeSpec, [],
                                     NewParseState);
parse_chunk({attribute, _Line, _AttributeName, _AttributeValue}, _Module,
            ParseState) ->
    ParseState;
parse_chunk({function, _Line, _FunName, _FunArity, FunCode}, _Module, ParseState) ->
    erlang:put(fun_name, _FunName),
    tester_value_collector:parse_expression(FunCode, ParseState);
parse_chunk({eof, _Line}, _Module, ParseState) ->
    ParseState.

-spec parse_type_log(any(), module(), tester_parse_state:state(), tuple()) ->
                            {type_spec() , tester_parse_state:state()}.
parse_type_log(Type, Module, ParseState, Info) ->
    try
        parse_type(Type, Module, ParseState)
    catch
        unknown_type ->
            ct:pal("~p:~p: failed to parse type ~p", [Module, Info, Type]),
            exit(foobar)%;
%%        error:Reason ->
%%            ct:pal("~p:~p: failed to parse type ~p (error:~p)", [Module, Info, Type, Reason]),
%%            exit(foobar)
    end.

-spec parse_type(any(), module(), tester_parse_state:state()) ->
                        {type_spec() , tester_parse_state:state()}.
parse_type({union_fun, FunSpecs}, Module, ParseState) ->
    {FunSpecs2, PS2} = lists:foldl(fun (FunType, {List, PS}) ->
                        {ParsedFunType, PS1 } = parse_type(FunType, Module, PS),
                        {[ParsedFunType | List], PS1}
                end, {[], ParseState}, FunSpecs),
    {{union_fun, FunSpecs2}, PS2};
parse_type({type, _Line, 'fun', [Arg, Result]}, Module, ParseState) ->
    {ArgType, ParseState2} = parse_type(Arg, Module, ParseState),
    {ResultType, ParseState3} = parse_type(Result, Module, ParseState2),
    {{'fun', ArgType, ResultType}, ParseState3};
parse_type({type, _Line, product, Types}, Module, ParseState) ->
    {TypeList, ParseState2} = parse_type_list(Types, Module, ParseState),
    {{product, TypeList}, ParseState2};
parse_type({type, _Line, tuple, any}, _Module, ParseState) ->
    {{tuple, {typedef, tester, test_any, []}}, ParseState};
parse_type({type, _Line, tuple, Types}, Module, ParseState) ->
    {TypeList, ParseState2} = parse_type_list(Types, Module, ParseState),
    {{tuple, TypeList}, ParseState2};
parse_type({type, _Line, list, [Type]}, Module, ParseState) ->
    {ListType, ParseState2} = parse_type(Type, Module, ParseState),
    {{list, ListType}, ParseState2};
parse_type({type, _Line, nonempty_list, [Type]}, Module, ParseState) ->
    {ListType, ParseState2} = parse_type(Type, Module, ParseState),
    {{nonempty_list, ListType}, ParseState2};
parse_type({type, _Line, nonempty_improper_list, Types = [_Type1, _Type2]}, Module, ParseState) ->
    {TypeList, ParseState2} = parse_type_list(Types, Module, ParseState),
    {{nonempty_improper_list, TypeList}, ParseState2};
parse_type({type, _Line, list, []}, _Module, ParseState) ->
    {{list, {typedef, tester, test_any, []}}, ParseState};
parse_type([], _Module, ParseState) ->
    {{list, {typedef, tester, test_any, []}}, ParseState};
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
parse_type({type, _Line, neg_integer, []}, _Module, ParseState) ->
    {neg_integer, ParseState};
parse_type({type, _Line, non_neg_integer, []}, _Module, ParseState) ->
    {non_neg_integer, ParseState};
parse_type({type, _Line, byte, []}, _Module, ParseState) ->
    {{range, {integer, 0}, {integer, 255}}, ParseState};
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
    {{typedef, tester, test_any, []}, ParseState};
parse_type({type, _Line, any}, _Module, ParseState) ->
    {{typedef, tester, test_any}, ParseState};
parse_type({type, _Line, atom, []}, _Module, ParseState) ->
    {atom, ParseState};
parse_type({type, _Line, arity, []}, _Module, ParseState) ->
    {arity, ParseState};
parse_type({type, _Line, binary, L}, _Module, ParseState) ->
    {{binary, L}, ParseState};
parse_type({type, _Line, pid, []}, _Module, ParseState) ->
    {pid, ParseState};
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
parse_type({type, _Line, no_return, []}, _Module, ParseState) ->
    {none, ParseState};
parse_type({type, _Line, reference, []}, _Module, ParseState) ->
    {reference, ParseState};
parse_type({type, _Line, term, []}, _Module, ParseState) ->
    {{typedef, tester, test_any, []}, ParseState};
parse_type({ann_type, _Line, [{var, _Line, _Varname}, Type]}, Module, ParseState) ->
    parse_type(Type, Module, ParseState);
parse_type({atom, _Line, Atom}, _Module, ParseState) ->
    {{atom, Atom}, ParseState};
parse_type({op, _Line1, '-', {integer,_Line2,Value}}, _Module, ParseState) ->
    {{integer, -Value}, ParseState};
parse_type({op, _Line1, '-', FirstType, {integer,_Line2,Value}}, Module, ParseState) ->
    {{op_diff, parse_type(FirstType, Module, ParseState), {integer, Value}}, ParseState};
parse_type({op, _Line1, 'bsl', FirstType, SecondType}, Module, ParseState) ->
    First = parse_type(FirstType, Module, ParseState),
    Second = parse_type(SecondType, Module, ParseState),
    {{op_bsl, First, Second}, ParseState};
parse_type({integer, _Line, Value}, _Module, ParseState) ->
    {{integer, Value}, ParseState};
parse_type({type, _Line, array, []}, _Module, ParseState) ->
    {{builtin_type, array_array, {typedef, tester, test_any, []}}, ParseState};
parse_type({type, _Line, dict, []}, _Module, ParseState) ->
    {{builtin_type, dict_dict, {typedef, tester, test_any, []},
      {typedef, tester, test_any, []}}, ParseState};
parse_type({type, _Line, queue, []}, _Module, ParseState) ->
    {{builtin_type, queue_queue, {typedef, tester, test_any, []}}, ParseState};
parse_type({type, _Line, gb_set, []}, _Module, ParseState) ->
    {{builtin_type, gb_sets_set, {typedef, tester, test_any, []}}, ParseState};
parse_type({type, _Line, gb_tree, []}, _Module, ParseState) ->
    {{builtin_type, gb_trees_tree, {typedef, tester, test_any, []},
      {typedef, tester, test_any, []}}, ParseState};
parse_type({type, _Line, set, []}, _Module, ParseState) ->
    {{builtin_type, set_set, {typedef, tester, test_any, []}}, ParseState};
parse_type({type, _Line, module, []}, _Module, ParseState) ->
    {{builtin_type, module}, ParseState};
parse_type({type, _Line, iodata, []}, _Module, ParseState) ->
    {{builtin_type, iodata}, ParseState};
parse_type({type, _Line, map, any}, _Module, ParseState) -> % Erlang R17
    {{builtin_type, map}, ParseState};
parse_type({type, _Line, mfa, []}, _Module, ParseState) ->
    {{tuple, [atom, atom, {range, {integer, 0}, {integer, 255}}]}, ParseState};
% array:array(Value)
parse_type({remote_type, _Line, [{atom, _Line2, array},
                                 {atom, _Line3, array}, [ValueType]]},
           Module, ParseState) ->
    {Value, ParseState2}   = parse_type(ValueType, Module, ParseState),
    {{builtin_type, array_array, Value}, ParseState2};
% dict:dict(Key,Value)
parse_type({remote_type, _Line, [{atom, _Line2, dict},
                                 {atom, _Line3, dict}, [KeyType, ValueType]]},
           Module, ParseState) ->
    {Key2, ParseState2}   = parse_type(KeyType, Module, ParseState),
    {Value2, ParseState3}   = parse_type(ValueType, Module, ParseState2),
    {{builtin_type, dict_dict, Key2, Value2}, ParseState3};
% queue:queue(Value)
parse_type({remote_type, _Line, [{atom, _Line2, queue},
                                 {atom, _Line3, queue}, [ValueType]]},
           Module, ParseState) ->
    {Value, ParseState2}   = parse_type(ValueType, Module, ParseState),
    {{builtin_type, queue_queue, Value}, ParseState2};
% gb_sets:set(Value)
parse_type({remote_type, _Line, [{atom, _Line2, gb_sets},
                                 {atom, _Line3, set}, [ValueType]]},
           Module, ParseState) ->
    {Value, ParseState2}   = parse_type(ValueType, Module, ParseState),
    {{builtin_type, gb_sets_set, Value}, ParseState2};
% gb_trees:tree(Key,Value)
parse_type({remote_type, _Line, [{atom, _Line2, gb_trees},
                                 {atom, _Line3, tree}, [KeyType, ValueType]]},
           Module, ParseState) ->
    {Key2, ParseState2}   = parse_type(KeyType, Module, ParseState),
    {Value2, ParseState3}   = parse_type(ValueType, Module, ParseState2),
    {{builtin_type, gb_trees_tree, Key2, Value2}, ParseState3};
% gb_trees:tree()
parse_type({remote_type, _Line, [{atom, _Line2, gb_trees},
                                 {atom, _Line3, tree}, []]},
           _Module, ParseState) ->
    {{builtin_type, gb_trees_tree, {typedef, tester, test_any, []},
      {typedef, tester, test_any, []}}, ParseState};
% gb_trees:iter(Key,Value)
parse_type({remote_type, _Line, [{atom, _Line2, gb_trees},
                                 {atom, _Line3, iter}, [KeyType, ValueType]]},
           Module, ParseState) ->
    {Key2, ParseState2}   = parse_type(KeyType, Module, ParseState),
    {Value2, ParseState3}   = parse_type(ValueType, Module, ParseState2),
    {{builtin_type, gb_trees_iter, Key2, Value2}, ParseState3};
% sets:set(Value)
parse_type({remote_type, _Line, [{atom, _Line2, sets},
                                 {atom, _Line3, set}, [ValueType]]},
           Module, ParseState) ->
    {Value, ParseState2}   = parse_type(ValueType, Module, ParseState),
    {{builtin_type, set_set, Value}, ParseState2};
parse_type({remote_type, _Line, [{atom, _Line2, TypeModule},
                                 {atom, _Line3, TypeName}, L]},
           _Module, ParseState) ->
    case tester_parse_state:is_known_type(TypeModule, TypeName, length(L), ParseState) of
        true ->
            {{typedef, TypeModule, TypeName, L}, ParseState};
        false ->
            {{typedef, TypeModule, TypeName, L},
             tester_parse_state:add_unknown_type(TypeModule, TypeName, length(L), ParseState)}
    end;
% why is this here? function() is no official type
parse_type({type, _Line, 'function', []}, _Module, ParseState) ->
    {{'function'}, ParseState};
parse_type({type, _Line, 'fun', []}, _Module, ParseState) ->
    {{'function'}, ParseState};
parse_type({type, _Line, record, [{atom, _Line2, TypeName}]}, Module, ParseState) ->
    {{record, Module, TypeName}, ParseState};
parse_type({type, _Line, record, [{atom, _Line2, TypeName} | Fields]}, Module,
           ParseState) ->
    {RecordType, ParseState2} = parse_type_list(Fields, Module, ParseState),
    {{record, Module, TypeName, RecordType}, ParseState2};
parse_type({typed_record_field, {record_field, _Line,
                                 {atom, _Line2, FieldName}}, Field}, Module,
           ParseState) ->
    {FieldType, ParseState2} = parse_type(Field, Module, ParseState),
    {{typed_record_field, FieldName, FieldType}, ParseState2};
parse_type({typed_record_field, {record_field, _Line,
                                 {atom, _Line2, FieldName}, _Default}, Field},
           Module, ParseState) ->
    {FieldType, ParseState2} = parse_type(Field, Module, ParseState),
    {{typed_record_field, FieldName, FieldType}, ParseState2};
parse_type({type, _, field_type, [{atom, _, FieldName}, Field]}, Module, ParseState) ->
    {FieldType, ParseState2} = parse_type(Field, Module, ParseState),
    {{field_type, FieldName, FieldType}, ParseState2};
parse_type({record_field, _Line, {atom, _Line2, FieldName}}, _Module, ParseState) ->
    {{untyped_record_field, FieldName}, ParseState};
parse_type({record_field, _Line, {atom, _Line2, FieldName}, _Default}, _Module,
           ParseState) ->
    {{untyped_record_field, FieldName}, ParseState};
parse_type(TypeSpecs, Module, ParseState) when is_list(TypeSpecs) ->
    case hd(TypeSpecs) of
        {typed_record_field, _, _} ->
            {RecordType, ParseState2} = parse_type_list(TypeSpecs,
                                                        Module, ParseState),
            {{record, RecordType}, ParseState2};
        {record_field, _, _} ->
            {RecordType, ParseState2} = parse_type_list(TypeSpecs,
                                                        Module, ParseState),
            {{record, RecordType}, ParseState2};
        {record_field, _, _, _} ->
            {RecordType, ParseState2} = parse_type_list(TypeSpecs,
                                                        Module, ParseState),
            {{record, RecordType}, ParseState2};
        _ ->
            ct:pal("potentially unknown type2: ~p~n", [TypeSpecs]),
            unknown
    end;
parse_type({var, _Line, Atom}, _Module, ParseState) when is_atom(Atom) ->
    {{var, Atom}, ParseState};
parse_type({type, _Line, constraint, _Constraint}, _Module, ParseState) ->
    {{constraint, nyi}, ParseState};
parse_type({type, _, bounded_fun, [FunType, ConstraintList]}, Module, ParseState) ->
    {InternalFunType, ParseState2} = parse_type(FunType, Module, ParseState),
    Foldl = fun (Constraint, {PartialConstraintList, ParseState2a}) ->
                    {InternalConstraint, ParseState2c} = parse_type(Constraint,
                                                                    Module,
                                                                    ParseState2a),
                    {[InternalConstraint | PartialConstraintList], ParseState2c}
            end,
    {Constraints, ParseState3} = lists:foldl(Foldl, {[], ParseState2}, ConstraintList),
    {{bounded_fun, InternalFunType, Constraints}, ParseState3};
parse_type({paren_type, _Line, [InnerType]}, Module, ParseState) ->
    parse_type(InnerType, Module, ParseState);
parse_type({type, _Line, identifier, L}, _Module, ParseState) when is_list(L) ->
    {{builtin_type, identifier}, ParseState};
parse_type({type, _Line, timeout, L}, _Module, ParseState) when is_list(L) ->
    {{builtin_type, timeout}, ParseState};
parse_type({type, _Line, bitstring, L}, _Module, ParseState) when is_list(L) ->
    {{builtin_type, bitstring}, ParseState};
parse_type({type, _Line, maybe_improper_list, L}, _Module, ParseState) when is_list(L) ->
    {{builtin_type, maybe_improper_list}, ParseState};
parse_type({user_type, Line, TypeName, L}, Module, ParseState) ->
    parse_type({type, Line, TypeName, L}, Module, ParseState);
parse_type({type, _Line, map, MapFields}, Module, ParseState) when is_list(MapFields) ->
    %% ct:pal("type assoc map ~p:~p~n~w~n~w~n~w~n", [Module, map, MapFields, erlang:get(current_module), _Line]),
    {Fields, NextParseState}
        = lists:foldl(fun (FieldType, {FieldList, State}) ->
                              case FieldType of
                                  {type, _, map_field_assoc, [NameType, Type]} ->
                                      {TheNameTypeSpec, NewParseState} = parse_type(NameType, Module, State),
                                      {TheTypeSpec, NewParseState2} = parse_type(Type, Module, NewParseState),
                                      {[{assoc_map_field, TheNameTypeSpec, TheTypeSpec} | FieldList], NewParseState2};
                                  {type, _, map_field_exact, [NameType, Type]} ->
                                      {TheNameTypeSpec, NewParseState} = parse_type(NameType, Module, State),
                                      {TheTypeSpec, NewParseState2} = parse_type(Type, Module, NewParseState),
                                      {[{exact_map_field, TheNameTypeSpec, TheTypeSpec} | FieldList], NewParseState2};
                                  _ ->
                                      ct:pal("unknown map field: ~p", [FieldType]),
                                      throw(parse_error)
                              end
                      end,
                      {[], ParseState}, MapFields),
    {{type_assoc_map, Fields}, NextParseState};
parse_type({type, _Line, TypeName, L}, Module, ParseState) ->
    % ct:pal("type1 ~p:~p~n~w~n~w~n~w~n", [Module, TypeName, L, erlang:get(current_module), _Line]),
    case tester_parse_state:is_known_type(Module, TypeName, length(L), ParseState) of
        true ->
            {{typedef, Module, TypeName, L}, ParseState};
        false ->
            {{typedef, Module, TypeName, L},
             tester_parse_state:add_unknown_type(Module, TypeName, length(L), ParseState)}
    end;
%% parse_type({ann_type,_Line,[Left,Right]}, _Module, ParseState) ->
%%     {{ann_type, [Left, Right]}, ParseState};
parse_type({union, L}, Module, ParseState) ->
    Foldl = fun (NextType, {TypeList, AParseState}) ->
                    {TheTypeSpec, NewParseState} = parse_type(NextType, Module, AParseState),
                    {[TheTypeSpec | TypeList], NewParseState}
            end,
    {Types, NextParseState} = lists:foldl(Foldl, {[], ParseState}, L),
    {{union, Types}, NextParseState};
parse_type(TypeSpec, Module, ParseState) ->
    ct:pal("unknown type ~p in module ~p~n", [TypeSpec, Module]),
    throw(unknown_type),
    {unknown, ParseState}.

-spec parse_type_list(list(type_spec()), module(), tester_parse_state:state()) ->
                             {list(type_spec()), tester_parse_state:state()}.
parse_type_list(List, Module, ParseState) ->
    case List of
        [] ->
            {[], ParseState};
        [Head | Tail] ->
            {Type, ParseState2} = parse_type(Head, Module, ParseState),
            {TypeList, ParseState3} = parse_type_list(Tail, Module, ParseState2),
            {[Type | TypeList], ParseState3}
    end.

parse_constraints([], Substitutions) ->
    Substitutions;
parse_constraints([ConstraintType | Rest], Substitutions) ->
    case ConstraintType of
        {type,_,constraint,[{atom,_,is_subtype},[{var,_,Variable},Type]]} ->
            case gb_trees:lookup({var, Variable}, Substitutions) of
                {value,Val} ->
                    case equal_types(Val, Type) of
                        true ->
                            NewSubstitutions = gb_trees:enter({var, Variable}, Type, Substitutions),
                            parse_constraints(Rest, NewSubstitutions);
                        false ->
                            throw({parse_error, Val})
                    end;
                none ->
                    NewSubstitutions = gb_trees:insert({var, Variable}, Type, Substitutions),
                    parse_constraints(Rest, NewSubstitutions)
            end;
        _ ->
            ct:pal("unknown constraint ~w", [ConstraintType]),
            parse_constraints(Rest, Substitutions)
    end.


% type equality minus line number
equal_types(Left, Right)  when is_list(Left) andalso is_list(Right) ->
    lists:all(fun ({L, R}) ->
                      equal_types(L, R)
              end, lists:zip(Left, Right));
equal_types({type,_, Type, LeftList}, {type,_, Type, RightList} ) ->
    equal_types(LeftList, RightList);
equal_types(_, _) ->
    false.

