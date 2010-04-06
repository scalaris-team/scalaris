%  Copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : tester.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : test generator
%%%
%%% Created :  30 March 2010 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(tester).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([test/4]).

% for unit tests
-export([create_value/3]).

-include_lib("unittest.hrl").
-include_lib("../include/scalaris.hrl").

-type(test_any() ::
   % | none()
   % | pid()
   % | port()
   % | ref()
       []
     | atom()
   % | binary()
     | float()
   % | Fun
     | integer()
     | 42
     | -1..1
     | list()
     | list(test_any())
   % | improper_list(test_any(), test_any()
   % | maybe_improper_list(test_any(), test_any())
     | tuple()
     | {}
     | {test_any(), test_any(), test_any()}
   % | Union
   % | Userdefined
           ).


-type(type_info() :: gb_tree()).
-type(builtin_type() ::
      dict
    | gb_tree
    | gb_set
    | module).

-ifdef(forward_or_recursive_types_are_not_allowed).
-type(record_field_type() ::
     {typed_record_field, atom(), any()}
   | {untyped_record_field, atom()}).

-type(tester_type() ::
      {'fun', any(), any()}
    | {product, [any()]}
    | {tuple, [any()]}
    | {tuple, {typedef, tester, test_any}}
    | {list, any()}
    | {nonempty_list, any()}
    | {range, {integer, integer()}, {integer, integer()}}
    | {union, [any()]}
    | {record, atom(), atom()}
    | integer
    | pos_integer
    | non_neg_integer
    | bool
    | binary
    | iolist
    | node
    | pid
    | port
    | reference
    | none
    | {typedef, module(), atom()}
    | atom
    | float
    | nil
    | {atom, atom()}
    | {integer, integer()}
    | {builtin_type, builtin_type()}
    | {record, list(record_field_type())}
     ).
-else.
-type(record_field_type() ::
     {typed_record_field, atom(), tester_type()}
   | {untyped_record_field, atom()}).

-type(tester_type() ::
      {'fun', tester_type(), tester_type()}
    | {product, [tester_type()]}
    | {tuple, [tester_type()]}
    | {tuple, {typedef, tester, test_any}}
    | {list, tester_type()}
    | {nonempty_list, tester_type()}
    | {range, {integer, integer()}, {integer, integer()}}
    | {union, [tester_type()]}
    | {record, atom(), atom()}
    | integer
    | pos_integer
    | non_neg_integer
    | bool
    | binary
    | iolist
    | node
    | pid
    | port
    | reference
    | none
    | {typedef, module(), atom()}
    | atom
    | float
    | nil
    | {atom, atom()}
    | {integer, integer()}
    | {builtin_type, builtin_type()}
    | {record, list(record_field_type())}
     ).
-endif.

list_length_min() -> 1.
list_length_max() -> 5.

integer_min() -> 1.
integer_max() -> 5.

test(Module, Func, Arity, Iterations) ->
    TypeInfo = collect_fun_info(Module, Func, Arity, gb_trees:empty()),
    run(Module, Func, Arity, Iterations, TypeInfo),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% collect necessary type information
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
collect_fun_info(Module, Func, Arity, TypeInfo) ->
    TypeInfo2 = case gb_trees:lookup({'fun', Module, Func, Arity}, TypeInfo) of
                    {value, _} ->
                        TypeInfo;
                    none ->
                        {ok, {Module, [{abstract_code, {raw_abstract_v1, AbstractCode}}]}}
                            = beam_lib:chunks(code:which(Module), [abstract_code]),
                        lists:foldl(fun (Chunk, InnerTypeInfo) ->
                                            parse_chunk(Chunk, Module, InnerTypeInfo)
                                    end, TypeInfo, AbstractCode)
                end,
    TypeInfo3 = case find_unknown_types(TypeInfo2) of
        [] ->
            ok;
        UnknownTypes ->
            io:format("~p~n", [UnknownTypes]),
            collect_unknown_type_infos(UnknownTypes, TypeInfo2)
    end,
    case gb_trees:lookup({'fun', Module, Func, Arity}, TypeInfo3) of
        {value, _} ->
            TypeInfo3;
        none ->
            io:format("unknown function ~p:~p/~p~n", [Module, Func, Arity]),
            halt(0)
    end.

collect_unknown_type_infos(UnknownTypes, TypeInfo) ->
    ct:pal("~p", [UnknownTypes]),
    %ct:pal("~p", [gb_trees:to_list(TypeInfo)]),
    NewTypeInfo = lists:foldl(fun ({type, Module, TypeName}, InnerTypeInfo) ->
                                      collect_type_info(Module, TypeName, InnerTypeInfo)
                              end, TypeInfo, UnknownTypes),
    case find_unknown_types(NewTypeInfo) of
        [] ->
            NewTypeInfo;
        NewUnknownTypes ->
            collect_unknown_type_infos(NewUnknownTypes, NewTypeInfo)
    end.

collect_type_info(Module, _Type, TypeInfo) ->
    {ok, {Module, [{abstract_code, {raw_abstract_v1, AbstractCode}}]}}
        = beam_lib:chunks(code:which(Module), [abstract_code]),
    lists:foldl(fun (Chunk, InnerTypeInfo) ->
                        parse_chunk(Chunk, Module, InnerTypeInfo)
                end, TypeInfo, AbstractCode).


parse_chunk({attribute, _Line, type, {{record, TypeName}, ATypeSpec, _List}}, Module, TypeInfo) ->
    TheTypeSpec = parse_type(ATypeSpec, Module),
    gb_trees:enter({type, Module, TypeName}, TheTypeSpec, TypeInfo);
parse_chunk({attribute, _Line, type, {TypeName, ATypeSpec, _List}}, Module, TypeInfo) ->
    TheTypeSpec = parse_type(ATypeSpec, Module),
    gb_trees:enter({type, Module, TypeName}, TheTypeSpec, TypeInfo);
parse_chunk({attribute, _Line, 'spec', {{FunName, FunArity}, [AFunSpec]}}, Module, TypeInfo) ->
    TheFunSpec = parse_type(AFunSpec, Module),
    gb_trees:enter({'fun', Module, FunName, FunArity}, TheFunSpec, TypeInfo);

parse_chunk({attribute, _Line, record, {TypeName, TypeList}}, Module, TypeInfo) ->
    TypeSpec = parse_type(TypeList, Module),
    gb_trees:enter({type, Module, TypeName}, TypeSpec, TypeInfo);
parse_chunk({attribute, _Line, _AttributeName, _AttributeValue}, _Module, TypeInfo) ->
    TypeInfo;
parse_chunk({function, _Line, _FunName, _FunArity, _FunCode}, _Module, TypeInfo) ->
    TypeInfo;
parse_chunk({eof, _Line}, _Module, TypeInfo) ->
    TypeInfo.

parse_type({type, _Line, 'fun', [Arg, Result]}, Module) ->
    {'fun', parse_type(Arg, Module), parse_type(Result, Module)};
parse_type({type, _Line, product, Types}, Module) ->
    {product, [parse_type(Type, Module) || Type <- Types]};
parse_type({type, _Line, tuple, any}, _Module) ->
    {tuple, {typedef, tester, test_any}};
parse_type({type, _Line, tuple, Types}, Module) ->
    {tuple, [parse_type(Type, Module) || Type <- Types]};
parse_type({type, _Line, list, [Type]}, Module) ->
    {list, parse_type(Type, Module)};
parse_type({type, _Line, nonempty_list, [Type]}, Module) ->
    {nonempty_list, parse_type(Type, Module)};
parse_type({type, _Line, list, []}, _Module) ->
    {list, {typedef, tester, test_any}};
parse_type({type, _Line, range, [Begin, End]}, Module) ->
    {range, parse_type(Begin, Module), parse_type(End, Module)};
parse_type({type, _Line, union, Types}, Module) ->
    {union, [parse_type(Type, Module) || Type <- Types]};
parse_type({type, _Line, integer, []}, _Module) ->
    integer;
parse_type({type, _Line, pos_integer, []}, _Module) ->
    pos_integer;
parse_type({type, _Line, non_neg_integer, []}, _Module) ->
    non_neg_integer;
parse_type({type, _Line, bool, []}, _Module) ->
    bool;
parse_type({type, _Line, char, []}, _Module) ->
    {range, {integer, 0}, {integer, 16#10ffff}};
parse_type({type, _Line, string, []}, _Module) ->
    {list, {range, {integer, 0}, {integer, 16#10ffff}}};
parse_type({type, _Line, number, []}, _Module) ->
    {union, [integer, float]};
parse_type({type, _Line, boolean, []}, _Module) ->
    bool;
parse_type({type, _Line, any, []}, _Module) ->
    {typedef, tester, test_any};
parse_type({type, _Line, atom, []}, _Module) ->
    atom;
parse_type({type, _Line, binary, []}, _Module) ->
    binary;
parse_type({type, _Line, pid, []}, _Module) ->
    pid;
parse_type({type, _Line, port, []}, _Module) ->
    port;
parse_type({type, _Line, float, []}, _Module) ->
    float;
parse_type({type, _Line, iolist, []}, _Module) ->
    iolist;
parse_type({type, _Line, nil, []}, _Module) ->
    nil;
parse_type({type, _Line, node, []}, _Module) ->
    node;
parse_type({type, _Line, none, []}, _Module) ->
    none;
parse_type({type, _Line, reference, []}, _Module) ->
    reference;
parse_type({type, _Line, term, []}, _Module) ->
    {typedef, tester, test_any};
parse_type({ann_type, _Line, [{var, _Line2, _Varname}, Type]}, Module) ->
    parse_type(Type, Module);
parse_type({atom, _Line, Atom}, _Module) ->
    {atom, Atom};
parse_type({integer, _Line, Value}, _Module) ->
    {integer, Value};
parse_type({type, _Line, dict, []}, _Module) ->
    {builtin_type, dict};
parse_type({type, _Line, gb_set, []}, _Module) ->
    {builtin_type, gb_set};
parse_type({type, _Line, gb_tree, []}, _Module) ->
    {builtin_type, gb_tree};
parse_type({type, _Line, module, []}, _Module) ->
    {builtin_type, module};
parse_type({remote_type, _Line, [{atom, _Line2, TypeModule}, {atom, _line3, TypeName}, []]}, _Module) ->
    {typedef, TypeModule, TypeName};
parse_type({type, _Line, TypeName, []}, Module) ->
    io:format("potentially unknown type1: ~p~n", [TypeName]),
    {typedef, Module, TypeName};
parse_type({type, _Line, record, [{atom, _Line2, TypeName}]}, Module) ->
    io:format("potentially unknown type1: ~p~n", [TypeName]),
    {record, Module, TypeName};
parse_type(TypeSpecs, Module) when is_list(TypeSpecs) ->
    case hd(TypeSpecs) of
        {typed_record_field, _, _} ->
            {record, parse_record_type(TypeSpecs, Module)};
        {record_field, _, _} ->
            {record, parse_record_type(TypeSpecs, Module)};
        {record_field, _, _, _} ->
            {record, parse_record_type(TypeSpecs, Module)};
        _ ->
            io:format("potentially unknown type2: ~p~n", [TypeSpecs]),
            unknown
    end;
parse_type(TypeSpec, _Module) ->
    io:format("potentially unknown type3: ~p~n", [TypeSpec]),
    unkown.

parse_record_type(TypeSpecs, Module) ->
    lists:map(fun (Type) ->
                      case Type of
                          {typed_record_field,
                           {record_field, _Line,
                            {atom, _Line2, FieldName}},
                           FieldType} ->
                              {typed_record_field, FieldName, parse_type(FieldType, Module)};
                          {typed_record_field,
                           {record_field, _Line,
                            {atom, _Line2, FieldName},
                            _Default},
                           FieldType} ->
                              {typed_record_field, FieldName, parse_type(FieldType, Module)};
                          {record_field, _Line, {atom, _Line2, FieldName}} ->
                              {untyped_record_field, FieldName};
                          {record_field, _Line, {atom, _Line2, FieldName}, _Default} ->
                              {untyped_record_field, FieldName};
                          _ ->
                              io:format("unknown record field: ~p~n", [Type]),
                              unknown_record_field
                      end
              end, TypeSpecs).

run(Module, Func, Arity, Iterations, TypeInfo) ->
    FunType = gb_trees:get({'fun', Module, Func, Arity}, TypeInfo),
    run_helper(Module, Func, Iterations, FunType, TypeInfo).

run_helper(_Module, _Func, 0, _FunType, _TypeInfo) ->
    ok;
run_helper(Module, Func, Iterations, {'fun', ArgType, _ResultType} = FunType, TypeInfo) ->
    Size = 30,
    Args = create_value(ArgType, Size, TypeInfo),
    case (try
              erlang:apply(Module, Func, Args)
          catch
              throw:Term -> {exception, Term};
              exit:Reason -> {exception, Reason};
              error:Reason -> {exception, {Reason, erlang:get_stacktrace()}} 
          end) of
        true ->
            run_helper(Module, Func, Iterations - 1, FunType, TypeInfo);
        X ->
            ?ct_fail("error ~p:~p(~p) failed with ~p~n", [Module, Func, Args, X])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% create a random value of the given type
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc create a random value of the given type
-spec(create_value/3 :: (tester_type(), pos_integer(), type_info()) -> term()).
create_value({product, Types}, Size, TypeInfo) ->
    NewSize = erlang:max(1, (Size - length(Types)) div length(Types)),
    [create_value(Type, NewSize, TypeInfo) || Type <- Types];
create_value({tuple, Types}, Size, TypeInfo) when is_list(Types) ->
    case Types of
        [] ->
            {};
        _ ->
            NewSize = erlang:max(1, (Size - length(Types)) div length(Types)),
            Values = [create_value(Type, NewSize, TypeInfo) || Type <- Types],
            erlang:list_to_tuple(Values)
    end;
create_value({tuple, {typedef, tester, test_any}}, Size, TypeInfo) ->
    Values = create_value({list, {typedef, tester, test_any}}, Size, TypeInfo),
    erlang:list_to_tuple(Values);
create_value({list, Type}, Size, TypeInfo) ->
    ListLength = erlang:min(Size, crypto:rand_uniform(list_length_min(),
                                               list_length_max() + 1)),
    case ListLength of
        0 ->
            [];
        _ ->
            NewSize = erlang:max(1, (Size - ListLength) div ListLength),
            [create_value(Type, NewSize, TypeInfo) || _ <- lists:seq(1, ListLength)]
    end;
create_value(integer, _Size, _TypeInfo) ->
    crypto:rand_uniform(integer_min(), integer_max() + 1);
create_value(pos_integer, _Size, _TypeInfo) ->
    crypto:rand_uniform(0, integer_max() + 1);
create_value({integer, Value}, _Size, _TypeInfo) ->
    Value;
create_value({atom, Value}, _Size, _TypeInfo) ->
    Value;
create_value(bool, _Size, _TypeInfo) ->
    case crypto:rand_uniform(0, 2) of
        0 ->
            false;
        1 ->
            true
    end;
create_value(nil, _Size, _TypeInfo) ->
    [];
create_value(atom, _Size, _TypeInfo) ->
    Atoms = [one, two, three, four],
    lists:nth(crypto:rand_uniform(1, length(Atoms) + 1), Atoms);
create_value(float, _Size, _TypeInfo) ->
    crypto:rand_uniform(-5, 5) * (crypto:rand_uniform(0, 30323) / 30323);
create_value({range, {integer, Low}, {integer, High}}, _Size, _TypeInfo) ->
    crypto:rand_uniform(Low, High + 1);
create_value({union, Types}, Size, TypeInfo) ->
    Length = length(Types),
    create_value(lists:nth(crypto:rand_uniform(1, Length + 1), Types), Size, TypeInfo);
create_value({typedef, Module, TypeName}, Size, TypeInfo) ->
    case gb_trees:lookup({type, Module, TypeName}, TypeInfo) of
        {value, TypeSpec} ->
            create_value(TypeSpec, Size, TypeInfo);
        none ->
            io:format("error: unknown type ~p:~p~n", [Module, TypeName])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% find unknown types in the provided type_info()
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc find types which are referenced in the typeinfo but not registered
-spec(find_unknown_types/1 :: (type_info()) -> list(tester_type())).
find_unknown_types(TypeInfo) ->
    UnknownTypes = lists:foldl(fun (Type, UnknownTypes) ->
                                       find_unknown_types_helper(Type, TypeInfo,
                                                                 UnknownTypes)
                               end, [],
                               gb_trees:values(TypeInfo)),
    lists:usort(UnknownTypes).

%% @doc helper for find_unknown_types/1
-spec(find_unknown_types_helper/3 :: (tester_type(), type_info(), list(tester_type())) ->
             list(tester_type())).
find_unknown_types_helper({'fun', Args, Result}, TypeInfo, UnknownTypes) ->
    find_unknown_types_helper(Result, TypeInfo,
                              find_unknown_types_helper(Args, TypeInfo,
                                                        UnknownTypes));
find_unknown_types_helper({product, Types}, TypeInfo, UnknownTypes) ->
    lists:foldl(fun (Type, InnerUnknownTypes) ->
                        find_unknown_types_helper(Type, TypeInfo, InnerUnknownTypes)
                end, UnknownTypes, Types);
find_unknown_types_helper({tuple, {typedef, tester, test_any}}, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper({tuple, Types}, TypeInfo, UnknownTypes) ->
    lists:foldl(fun (Type, InnerUnknownTypes) ->
                        find_unknown_types_helper(Type, TypeInfo, InnerUnknownTypes)
                end, UnknownTypes, Types);
find_unknown_types_helper({list, Type}, TypeInfo, UnknownTypes) ->
    find_unknown_types_helper(Type, TypeInfo, UnknownTypes);
find_unknown_types_helper({nonempty_list, Type}, TypeInfo, UnknownTypes) ->
    find_unknown_types_helper(Type, TypeInfo, UnknownTypes);
find_unknown_types_helper({range, {integer, _A}, {integer, _B}}, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper({union, Types}, TypeInfo, UnknownTypes) ->
    lists:foldl(fun (Type, InnerUnknownTypes) ->
                        find_unknown_types_helper(Type, TypeInfo, InnerUnknownTypes)
                end, UnknownTypes, Types);
find_unknown_types_helper(integer, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper({integer, _A}, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(pos_integer, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(non_neg_integer, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(binary, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(bool, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(atom, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(iolist, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper({atom, _Atom}, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(float, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(pid, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(port, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(reference, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(none, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(nil, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper(node, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper({builtin_type, _Type}, _TypeInfo, UnknownTypes) ->
    UnknownTypes;
find_unknown_types_helper({record, Module, TypeName}, TypeInfo, UnknownTypes) ->
    case gb_trees:lookup({type, Module, TypeName}, TypeInfo) of
        {value, _} ->
            UnknownTypes;
        none ->
            [{type, Module, TypeName} | UnknownTypes]
    end;
find_unknown_types_helper({record, Fields}, TypeInfo, UnknownTypes) ->
    lists:foldl(fun (Field, InnerUnknownTypes) ->
                        case Field of
                            {typed_record_field, _, Type} ->
                                find_unknown_types_helper(Type, TypeInfo, InnerUnknownTypes);
                            {untyped_record_field, _} ->
                                InnerUnknownTypes
                        end
                end, UnknownTypes, Fields);
find_unknown_types_helper({typedef, Module, TypeName}, TypeInfo, UnknownTypes) ->
    case gb_trees:lookup({type, Module, TypeName}, TypeInfo) of
        {value, _} ->
            UnknownTypes;
        none ->
            [{type, Module, TypeName} | UnknownTypes]
    end.
