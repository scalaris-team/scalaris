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

-include("tester.hrl").
-include("unittest.hrl").

-spec test/4 :: (module(), atom(), non_neg_integer(), non_neg_integer()) -> ok.
test(Module, Func, Arity, Iterations) ->
    EmptyParseState = tester_parse_state:new_parse_state(),
    ParseState = try collect_fun_info(Module, Func, Arity,
                                      EmptyParseState)
    catch
        throw:Term2 -> ?ct_fail("exception (throw) in ~p:~p(): ~p~n", [Module, Func, {exception, Term2}]);
        % special handling for exits that come from a ct:fail() call:
        exit:{test_case_failed, Reason2} -> ?ct_fail("error ~p:~p() failed with ~p~n", [Module, Func, Reason2]);
        exit:Reason2 -> ?ct_fail("exception (exit) in ~p:~p(): ~p~n", [Module, Func, {exception, Reason2}]);
        error:Reason2 -> ?ct_fail("exception (error) in ~p:~p(): ~p~n", [Module, Func, {exception, {Reason2, erlang:get_stacktrace()}}])
    end,
    run(Module, Func, Arity, Iterations, ParseState),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% collect necessary type information
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec collect_fun_info/4 :: (module(), atom(), non_neg_integer(), tester_parse_state:state()) -> tester_parse_state:state().
collect_fun_info(Module, Func, Arity, ParseState) ->
    ParseState2 =
        case tester_parse_state:lookup_type({'fun', Module, Func, Arity}, ParseState) of
            {value, _} -> ParseState;
            none ->
                ModuleFile = code:where_is_file(atom_to_list(Module) ++ ".beam"),
                {ok, {Module, [{abstract_code, {_AbstVersion, AbstractCode}}]}}
                    = beam_lib:chunks(ModuleFile, [abstract_code]),
                lists:foldl(fun(Chunk, InnerParseState) ->
                                    parse_chunk(Chunk, Module, InnerParseState)
                            end, ParseState, AbstractCode)
        end,
    ParseState3 = case tester_parse_state:has_unknown_types(ParseState2) of
                      false -> ParseState2;
                      true  -> collect_unknown_type_infos(ParseState2, 100)
                  end,
    case tester_parse_state:lookup_type({'fun', Module, Func, Arity}, ParseState3) of
        {value, _} -> tester_parse_state:finalize(ParseState3);
        none -> ?ct_fail("unknown function ~p:~p/~p~n", [Module, Func, Arity])
    end.

-spec collect_unknown_type_infos(tester_parse_state:state(), non_neg_integer()) -> tester_parse_state:state().
collect_unknown_type_infos(ParseState, 0) ->
    {_, UnknownTypes} = tester_parse_state:get_unknown_types(ParseState),
    ct:pal("warning still looking for unknown types: ~p~n", [UnknownTypes]),
    collect_unknown_type_infos(ParseState, 100);
collect_unknown_type_infos(ParseState, Counter) ->
    {_, UnknownTypes} = tester_parse_state:get_unknown_types(ParseState),
    ParseState2 = tester_parse_state:reset_unknown_types(ParseState),
    ParseState3 = lists:foldl(fun({type, Module, TypeName}, InnerParseState) ->
                                      collect_type_info(Module, TypeName, InnerParseState)
                              end, ParseState2, UnknownTypes),
    case tester_parse_state:has_unknown_types(ParseState3) of
        false -> ParseState3;
        true  -> collect_unknown_type_infos(ParseState3, Counter - 1)
    end.

-spec collect_type_info/3 :: (module(), atom(), tester_parse_state:state()) -> tester_parse_state:state().
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


-spec parse_chunk/3 :: (any(), module(), tester_parse_state:state()) -> tester_parse_state:state().
parse_chunk({attribute, _Line, type, {{record, TypeName}, ATypeSpec, _List}}, Module, ParseState) ->
    {TheTypeSpec, NewParseState} = parse_type(ATypeSpec, Module, ParseState),
    tester_parse_state:add_type_spec({type, Module, TypeName}, TheTypeSpec, NewParseState);
parse_chunk({attribute, _Line, type, {TypeName, ATypeSpec, _List}}, Module, ParseState) ->
    {TheTypeSpec, NewParseState} = parse_type(ATypeSpec, Module, ParseState),
    tester_parse_state:add_type_spec({type, Module, TypeName}, TheTypeSpec, NewParseState);
parse_chunk({attribute, _Line, opaque, {TypeName, ATypeSpec, _List}}, Module, ParseState) ->
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

-spec parse_type/3 :: (any(), module(), tester_parse_state:state()) -> {type_spec() , tester_parse_state:state()}.
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
parse_type({type, _Line, no_return, []}, _Module, ParseState) ->
    {none, ParseState};
parse_type({type, _Line, reference, []}, _Module, ParseState) ->
    {reference, ParseState};
parse_type({type, _Line, term, []}, _Module, ParseState) ->
    {{typedef, tester, test_any}, ParseState};
parse_type({ann_type, _Line, [{var, _Line2, _Varname}, Type]}, Module, ParseState) ->
    parse_type(Type, Module, ParseState);
parse_type({atom, _Line, Atom}, _Module, ParseState) ->
    {{atom, Atom}, ParseState};
parse_type({op, _Line1,'-',{integer,_Line2,Value}}, _Module, ParseState) ->
    {{integer, -Value}, ParseState};
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
parse_type({type, _Line, iodata, []}, _Module, ParseState) ->
    {{builtin_type, iodata}, ParseState};
parse_type({type, _Line, mfa, []}, _Module, ParseState) ->
    {{tuple, [atom, atom, {range, {integer, 0}, {integer, 255}}]}, ParseState};
parse_type({remote_type, _Line, [{atom, _Line2, TypeModule}, {atom, _line3, TypeName}, []]}, _Module, ParseState) ->
    case tester_parse_state:is_known_type(TypeModule, TypeName, ParseState) of
        true ->
            {{typedef, TypeModule, TypeName}, ParseState};
        false ->
            {{typedef, TypeModule, TypeName}, tester_parse_state:add_unknown_type(TypeModule, TypeName, ParseState)}
    end;
parse_type({type, _Line, 'function', []}, _Module, ParseState) ->
    {{'function'}, ParseState};
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
parse_type({type, _Line, record, [{atom, _Line2, TypeName} | Fields]}, Module, ParseState) ->
    {RecordType, ParseState2} = parse_type_list(Fields, Module, ParseState),
    {{record, Module, TypeName, RecordType}, ParseState2};
parse_type({typed_record_field, {record_field, _Line, {atom, _Line2, FieldName}}, Field}, Module, ParseState) ->
    {FieldType, ParseState2} = parse_type(Field, Module, ParseState),
    {{typed_record_field, FieldName, FieldType}, ParseState2};
parse_type({typed_record_field, {record_field, _Line, {atom, _Line2, FieldName}, _Default}, Field}, Module, ParseState) ->
    {FieldType, ParseState2} = parse_type(Field, Module, ParseState),
    {{typed_record_field, FieldName, FieldType}, ParseState2};
parse_type({type, _, field_type, [{atom, _, FieldName}, Field]}, Module, ParseState) ->
    {FieldType, ParseState2} = parse_type(Field, Module, ParseState),
    {{field_type, FieldName, FieldType}, ParseState2};
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
            ct:pal("potentially unknown type2: ~p~n", [TypeSpecs]),
            unknown
    end;
parse_type({var, _Line, Atom}, _Module, ParseState) when is_atom(Atom) ->
    {{tuple, {typedef, tester, test_any}}, ParseState};
parse_type(TypeSpec, Module, ParseState) ->
    ct:pal("unknown type ~p in module ~p", [TypeSpec, Module]),
    {unkown, ParseState}.

-spec parse_type_list/3 :: (list(type_spec()), module(), tester_parse_state:state()) -> {list(type_spec()), tester_parse_state:state()}.
parse_type_list(List, Module, ParseState) ->
    case List of
        [] ->
            {[], ParseState};
        [Head | Tail] ->
            {Type, ParseState2} = parse_type(Head, Module, ParseState),
            {TypeList, ParseState3} = parse_type_list(Tail, Module, ParseState2),
            {[Type | TypeList], ParseState3}
    end.

-spec run/5 :: (module(), atom(), non_neg_integer(), non_neg_integer(), tester_parse_state:state()) -> ok.
run(Module, Func, Arity, Iterations, ParseState) ->
    {value, FunType} = tester_parse_state:lookup_type({'fun', Module, Func, Arity}, ParseState),
    run_helper(Module, Func, Arity, Iterations, FunType, ParseState).

-spec run_helper/6 :: (module(), atom(), non_neg_integer(), non_neg_integer(),
                       {'fun', type_spec(), type_spec()}, tester_parse_state:state()) -> ok.
run_helper(_Module, _Func, _Arity, 0, _FunType, _TypeInfos) ->
    ok;
run_helper(Module, Func, 0, Iterations, {'fun', _ArgType, _ResultType} = FunType, TypeInfos) ->
    try erlang:apply(Module, Func, []) of
        true ->
            run_helper(Module, Func, 0, Iterations - 1, FunType, TypeInfos);
        X ->
            ?ct_fail("error ~p:~p(~p) failed with ~p~n", [Module, Func, [], X])
    catch
        throw:Term -> ?ct_fail("exception (throw) in ~p:~p(~p): ~p~n",
                               [Module, Func, [], {exception, Term}]);
        % special handling for exits that come from a ct:fail() call:
        exit:{test_case_failed, Reason} ->
            ?ct_fail("error ~p:~p(~p) failed with ~p~n",
                     [Module, Func, [], Reason]);
        exit:Reason ->
            ?ct_fail("exception (exit) in ~p:~p(~p): ~p~n",
                                [Module, Func, [], {exception, Reason}]);
        error:Reason ->
            ?ct_fail("exception (error) in ~p:~p(~p): ~p~n",
                     [Module, Func, [], {exception, {Reason, erlang:get_stacktrace()}}])
    end;
run_helper(Module, Func, Arity, Iterations, {'fun', ArgType, _ResultType} = FunType, ParseState) ->
    Size = 30,
    Args = try tester_value_creator:create_value(ArgType, Size, ParseState)
           catch
               throw:Term ->
                   ?ct_fail("exception (throw) in ~p:~p(): ~p~n",
                            [Module, Func, {exception, Term}]);
               exit:Reason ->
                   ?ct_fail("exception (exit) in ~p:~p(): ~p~n",
                            [Module, Func, {exception, Reason}]);
               error:Reason ->
                   ?ct_fail("exception (error) in ~p:~p(): ~p~n",
                            [Module, Func, {exception, {Reason, erlang:get_stacktrace()}}])
    end,
    try erlang:apply(Module, Func, Args) of
        true ->
            run_helper(Module, Func, Arity, Iterations - 1, FunType, ParseState);
        X ->
            ?ct_fail("error ~p:~p(~p) failed with ~p~n", [Module, Func, Args, X])
    catch
        throw:Term2 ->
            ?ct_fail("exception (throw) in ~p:~p(~p): ~p~n",
                     [Module, Func, Args, {exception, Term2}]);
        % special handling for exits that come from a ct:fail() call:
        exit:{test_case_failed, Reason2} ->
            ?ct_fail("error ~p:~p(~p) failed with ~p~n",
                     [Module, Func, Args, Reason2]);
        exit:Reason2 ->
            ?ct_fail("exception (exit) in ~p:~p(~p): ~p~n",
                     [Module, Func, Args, {exception, Reason2}]);
        error:Reason2 ->
            ?ct_fail("exception (error) in ~p:~p(~p): ~p~n",
                     [Module, Func, Args, {exception, {Reason2, erlang:get_stacktrace()}}])
    end.
