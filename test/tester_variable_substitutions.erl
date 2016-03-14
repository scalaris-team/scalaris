%  @copyright 2010-2016 Zuse Institute Berlin

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
%% @doc    variable substitutions for tester
%% @end
-module(tester_variable_substitutions).
-author('schuett@zib.de').

-include("unittest.hrl").
-include("tester.hrl").

-export([substitute/2, substitutions_from_list/2]).

-type fun_spec() :: any().

-spec substitute(fun_spec(), gb_trees:tree({var, atom()}, term())) -> fun_spec().
substitute(FunSpecs, Substitutions) when is_list(FunSpecs)->
    [substitute(FunSpec, Substitutions) ||  FunSpec <- FunSpecs];
% type variable
substitute(Var = {var,_Line,VarName}, Substitutions) ->
    case gb_trees:lookup({var,VarName}, Substitutions) of
        {value, Substitution} -> Substitution;
        none ->
            %% VarName is unknown see e.g. Key at orddict:is_key/2
            Var
    end;

substitute({type, _Line,bounded_fun, [FunType, _Constraints]}, Substitutions) ->
    substitute(FunType, Substitutions);

% generic types
substitute({type, Line,TypeType, Types}, Substitutions) ->
    Types2 = substitute(Types, Substitutions),
    {type,Line,TypeType,Types2};

substitute({tuple, Types}, Substitutions) ->
    {tuple, substitute(Types, Substitutions)};

substitute({builtin_type, Type}, _Substitutions) ->
    {builtin_type, Type};

substitute({union, _Line, Types}, Substitutions) ->
    {union, substitute(Types, Substitutions)};

substitute({union, Types}, Substitutions) ->
    {union, substitute(Types, Substitutions)};

substitute({typedef, Module, Type, Params}, Substitutions) ->
    {typedef, Module, Type, substitute(Params, Substitutions)};

% user types
substitute({user_type, Line, TypeType, Types}, Substitutions) ->
    Types2 = substitute(Types, Substitutions),
    {type,Line,TypeType,Types2};

% special types
substitute({ann_type,_Line,[{var, _Line2, _Name}, Type]}, Substitutions) ->
    substitute(Type, Substitutions);
substitute({ann_type,Line,[Left,Right]}, Substitutions) ->
    Left2 = substitute(Left, Substitutions),
    Right2 = substitute(Right, Substitutions),
    {ann_type,Line,[Left2,Right2]};
substitute({remote_type,Line,[Left,Right,L]}, Substitutions) ->
    Left2 = substitute(Left, Substitutions),
    Right2 = substitute(Right, Substitutions),
    L2 = [ substitute(Element, Substitutions) || Element <- L],
    {remote_type,Line,[Left2,Right2,L2]};
substitute(any, _Substitutions) ->
    any;

substitute({paren_type, Line,[{type,_,union, Vars}]}, Substitutions) ->
    substitute({union, Line, Vars}, Substitutions);

% value types
substitute({atom,Line,Value}, _Substitutions) ->
    {atom,Line,Value};
substitute({integer,Line,Value}, _Substitutions) ->
    {integer,Line,Value};

substitute(Unknown, Substitutions) ->
    ct:pal("Unknown substitution: ~w", [Unknown]),
    ct:pal("Known substitutions: ~w", [gb_trees:to_list(Substitutions)]),
    throw({subst_error, unknown_expression}),
    exit(foobar).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% substitutions_from_list
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec substitutions_from_list(list({var, integer(), atom()}), list(term()))
                             -> gb_trees:tree({var, atom()}, term()).
substitutions_from_list(VarList, TypeList) ->
    Tree = gb_trees:empty(),
    substitutions_from_list_(VarList, TypeList, Tree).


substitutions_from_list_([], [], Tree) ->
    Tree;
substitutions_from_list_([{var,_Line,V} | VRest], [Type | TRest], Tree) ->
    substitutions_from_list_(VRest, TRest, gb_trees:enter({var, V}, Type, Tree)).

