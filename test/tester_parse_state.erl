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
%% @doc    value collector for test generator
%% @end
%% @version $Id$
-module(tester_parse_state).
-author('schuett@zib.de').
-vsn('$Id$').

-export([new_parse_state/0,

         % add fun info to state
         find_fun_info/4,

         get_unknown_types/1, has_unknown_types/1,


         % add types
         add_type_spec/4, add_unknown_type/4,

         % add values
         add_atom/2, add_binary/2, add_float/2, add_integer/2, add_string/2,

         % get values
         get_atoms/1, get_binaries/1, get_floats/1,
         get_strings/1, get_non_empty_strings/1,
         get_integers/1, get_pos_integers/1, get_neg_integers/1, get_non_neg_integers/1,

         reset_unknown_types/1,

         is_known_type/4, lookup_type/2, lookup_fun_type/2,
         
         % compact state
         finalize/1]).

-include("unittest.hrl").
-include("tester.hrl").

-export_type([state/0]).

-record(parse_state,
        {type_infos        = gb_trees:empty() :: gb_trees:tree(type_name(), {var_list(), type_spec()}),
         unknown_types     = gb_sets:new()    :: gb_sets:set(type_name()) | {Length::non_neg_integer(), [type_name()]},
         atoms             = gb_sets:new()    :: gb_sets:set(atom()) | {Length::non_neg_integer(), [atom()]},
         binaries          = gb_sets:new()    :: gb_sets:set(binary()) | {Length::non_neg_integer(), [binary()]},
         integers          = gb_sets:new()    :: gb_sets:set(integer()) | {Length::non_neg_integer(), [integer()]},
         pos_integers      = null             :: null     | {Length::non_neg_integer(), [pos_integer()]},
         neg_integers      = null             :: null     | {Length::non_neg_integer(), [neg_integer()]},
         non_neg_integers  = null             :: null     | {Length::non_neg_integer(), [non_neg_integer()]},
         floats            = gb_sets:new()    :: gb_sets:set(float()) | {Length::non_neg_integer(), [float()]},
         non_empty_strings = gb_sets:new()    :: gb_sets:set(nonempty_string()) | {Length::non_neg_integer(), [nonempty_string()]}
        }).
-opaque state() :: #parse_state{}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% find fun info
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec find_fun_info(module(), atom(), non_neg_integer(), state()) -> state().
find_fun_info(Module, Func, Arity, ParseState) ->
    _ParseState2 = try tester_collect_function_info:collect_fun_info(Module,
                                                                    Func,
                                                                    Arity,
                                                                    ParseState)
    catch
        throw:Term2 -> ?ct_fail("exception (throw) in ~p:~p(): ~p~n",
                                [Module, Func,
                                 {exception, {Term2, erlang:get_stacktrace(),
                                              util:get_linetrace()}}]);
        % special handling for exits that come from a ct:fail() call:
        exit:{test_case_failed, Reason2} ->
            ?ct_fail("error ~p:~p/~p failed with ~p~n",
                     [Module, Func, Arity, {Reason2, erlang:get_stacktrace(),
                                            util:get_linetrace()}]);
        exit:Reason2 -> ?ct_fail("exception (exit) in ~p:~p(): ~p~n",
                                 [Module, Func,
                                  {exception, {Reason2, erlang:get_stacktrace(),
                                               util:get_linetrace()}}]);
        error:Reason2 -> ?ct_fail("exception (error) in ~p:~p(): ~p~n",
                                  [Module, Func,
                                   {exception, {Reason2, erlang:get_stacktrace(),
                                                util:get_linetrace()}}])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% parse state
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec new_parse_state() -> state().
new_parse_state() ->
    #parse_state{unknown_types = gb_sets:singleton({type, tester, test_any, 0})}.

-spec has_unknown_types(state()) -> boolean().
has_unknown_types(#parse_state{unknown_types=UnknownTypes}) ->
    case gb_sets:is_set(UnknownTypes) of
        true -> not gb_sets:is_empty(UnknownTypes);
        _    -> element(1, UnknownTypes) =/= 0
    end.

-spec get_unknown_types(state()) -> {Length::non_neg_integer(), [type_name()]}.
get_unknown_types(#parse_state{unknown_types=UnknownTypes}) ->
    case gb_sets:is_set(UnknownTypes) of
        true -> UnknownTypesList = gb_sets:to_list(UnknownTypes),
                {gb_sets:size(UnknownTypes), UnknownTypesList};
        _    -> UnknownTypes
    end.

-spec get_atoms(state()) -> {Length::non_neg_integer(), [atom()]}.
get_atoms(#parse_state{atoms=Atoms}) ->
    case gb_sets:is_set(Atoms) of
        true -> AtomsList = gb_sets:to_list(Atoms),
                {gb_sets:size(Atoms), AtomsList};
        _    -> Atoms
    end.

-spec get_binaries(state()) -> {Length::non_neg_integer(), [binary()]}.
get_binaries(#parse_state{binaries=Binaries}) ->
    case gb_sets:is_set(Binaries) of
        true -> BinariesList = gb_sets:to_list(Binaries),
                {gb_sets:size(Binaries), BinariesList};
        _    -> Binaries
    end.

-spec get_floats(state()) -> {Length::non_neg_integer(), [float()]}.
get_floats(#parse_state{floats=Floats}) ->
    case gb_sets:is_set(Floats) of
        true -> FloatsList = gb_sets:to_list(Floats),
                {gb_sets:size(Floats), FloatsList};
        _    -> Floats
    end.

-spec get_integers(state()) -> {Length::non_neg_integer(), [integer()]}.
get_integers(#parse_state{integers=Integers}) ->
    case gb_sets:is_set(Integers) of
        true -> IntegerList = gb_sets:to_list(Integers),
                {gb_sets:size(Integers), IntegerList};
        _    -> Integers
    end.

-spec get_pos_integers(state()) -> {Length::non_neg_integer(), Integers::[pos_integer()]}.
get_pos_integers(#parse_state{integers=Integers, pos_integers=null}) ->
    IntegerList = [I || I <- gb_sets:to_list(Integers), I > 0],
    {erlang:length(IntegerList), IntegerList};
get_pos_integers(#parse_state{pos_integers=PosIntegers}) ->
    PosIntegers.

-spec get_neg_integers(state()) -> {Length::non_neg_integer(), Integers::[neg_integer()]}.
get_neg_integers(#parse_state{integers=Integers, neg_integers=null}) ->
    IntegerList = [I || I <- gb_sets:to_list(Integers), I < 0],
    {erlang:length(IntegerList), IntegerList};
get_neg_integers(#parse_state{neg_integers=NegIntegers}) ->
    NegIntegers.

-spec get_non_neg_integers(state()) -> {Length::non_neg_integer(), [non_neg_integer()]}.
get_non_neg_integers(#parse_state{integers=Integers, non_neg_integers=null}) ->
    IntegerList = [I || I <- gb_sets:to_list(Integers), I >= 0],
    {erlang:length(IntegerList), IntegerList};
get_non_neg_integers(#parse_state{non_neg_integers=NonNegIntegers}) ->
    NonNegIntegers.

-spec get_strings(state()) -> {Length::non_neg_integer(), [string()]}.
get_strings(#parse_state{non_empty_strings=Strings}) ->
    case gb_sets:is_set(Strings) of
        true -> StringList = ["" | gb_sets:to_list(Strings)],
                {gb_sets:size(Strings) + 1, StringList};
        _    -> {L, S} = Strings,
                {L + 1, ["" | S]}
    end.

-spec get_non_empty_strings(state()) -> {Length::non_neg_integer(), [nonempty_string()]}.
get_non_empty_strings(#parse_state{non_empty_strings=Strings}) ->
    case gb_sets:is_set(Strings) of
        true -> StringsList = gb_sets:to_list(Strings),
                {gb_sets:size(Strings), StringsList};
        _    -> Strings
    end.

-spec add_type_spec(type_name(), type_spec(), var_list(), state()) -> state().
add_type_spec(TypeName, TypeSpec, VarList, #parse_state{type_infos=TypeInfos} = ParseState) ->
    NewTypeInfos = gb_trees:enter(TypeName, {var_type, VarList, TypeSpec}, TypeInfos),
    ParseState#parse_state{type_infos=NewTypeInfos}.

-spec add_unknown_type(module(), atom(), arity(), state()) -> state().
add_unknown_type(TypeModule, TypeName, Arity, #parse_state{unknown_types=UnknownTypes} = ParseState) ->
    ParseState#parse_state{unknown_types=
                           gb_sets:add_element({type, TypeModule, TypeName, Arity},
                                               UnknownTypes)}.

-spec reset_unknown_types(state()) -> state().
reset_unknown_types(ParseState) ->
    ParseState#parse_state{unknown_types=gb_sets:new()}.

-spec is_known_type(module(), atom(), arity(), state()) -> boolean().
is_known_type(TypeModule, TypeName, Arity, #parse_state{type_infos=TypeInfos}) ->
    gb_trees:is_defined({type, TypeModule, TypeName, Arity}, TypeInfos).

-spec add_atom(atom(), state()) -> state().
add_atom(Atom, #parse_state{atoms=Atoms} = ParseState) ->
    ParseState#parse_state{atoms=gb_sets:add_element(Atom, Atoms)}.

-spec add_binary(binary(), state()) -> state().
add_binary(Binary, #parse_state{binaries=Binaries} = ParseState) ->
    ParseState#parse_state{binaries=gb_sets:add_element(Binary, Binaries)}.

-spec add_float(float(), state()) -> state().
add_float(Float, #parse_state{floats=Floats} = ParseState) ->
    ParseState#parse_state{floats=gb_sets:add_element(Float, Floats)}.

-spec add_integer(integer(), state()) -> state().
add_integer(Integer, #parse_state{integers=Integers} = ParseState) ->
    ParseState#parse_state{integers=gb_sets:add_element(Integer, Integers)}.

-spec add_string(string(), state()) -> state().
add_string("", ParseState) ->
    ParseState;
add_string(String, #parse_state{non_empty_strings=Strings} = ParseState) ->
    ParseState#parse_state{non_empty_strings=gb_sets:add_element(String, Strings)}.

-spec lookup_type(type_name(), state()) -> {value, type_spec()} | none.
lookup_type(Type, #parse_state{type_infos=TypeInfos}) ->
    gb_trees:lookup(Type, TypeInfos).

-spec lookup_fun_type(type_name(), state()) -> 
                             {value, {var_type, [], {union_fun, [test_fun_type(),...]}}} | none.
lookup_fun_type(Type, #parse_state{type_infos=TypeInfos}) ->
    gb_trees:lookup(Type, TypeInfos).

%% @doc Compact the state for use during value creation. Do this after having
%%      collected all values in order to increase performance!
-spec finalize(state()) -> state().
finalize(#parse_state{unknown_types=UnknownTypes, atoms=Atoms,
                      binaries=Binaries, integers=Integers, floats=Floats,
                      non_empty_strings=Strings} = ParseState) ->
    IntegersList = gb_sets:to_list(Integers),
    NonNegIntegersList = [I || I <- IntegersList, I >= 0],
    PosIntegersList = [I || I <- NonNegIntegersList, I > 0],
    NegIntegersList = [I || I <- IntegersList, I < 0],
    UnknownTypesList = gb_sets:to_list(UnknownTypes),
    AtomsList = gb_sets:to_list(Atoms),
    BinariesList = gb_sets:to_list(Binaries),
    FloatsList = gb_sets:to_list(Floats),
    StringsList = gb_sets:to_list(Strings),
    ParseState#parse_state{unknown_types     = {gb_sets:size(UnknownTypes), UnknownTypesList},
                           atoms             = {gb_sets:size(Atoms), AtomsList},
                           binaries          = {gb_sets:size(Binaries), BinariesList},
                           integers          = {gb_sets:size(Integers), IntegersList},
                           pos_integers      = {erlang:length(PosIntegersList), PosIntegersList},
                           neg_integers      = {erlang:length(NegIntegersList), NegIntegersList},
                           non_neg_integers  = {erlang:length(NonNegIntegersList), NonNegIntegersList},
                           floats            = {gb_sets:size(Floats), FloatsList},
                           non_empty_strings = {gb_sets:size(Strings), StringsList}}.
