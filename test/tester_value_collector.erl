%  @copyright 2010-2011 Zuse Institute Berlin

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
%%         see http://www.erlang.org/doc/apps/erts/absform.html
%% @end
%% @version $Id$
-module(tester_value_collector).
-author('schuett@zib.de').
-vsn('$Id$').

-include("unittest.hrl").

-export([parse_expression/2]).

-spec parse_expression(any(), any()) -> any().
parse_expression(Expr, State) ->
    try
        parse_expression_intern(Expr, State)
    catch
        unknown_expression ->
            ct:pal("failed to parse expression ~p", [Expr]),
            exit(foobar)%;
        %error:Reason ->
        %    ct:pal("failed to parse expression ~p (error:~p)", [Expr, Reason]),
        %    exit(foobar)
    end.

-spec parse_expression_intern(any(), any()) -> any().
parse_expression_intern(Clauses, ParseState) when is_list(Clauses) ->
    lists:foldl(fun parse_expression_intern/2, ParseState, Clauses);
% binary comprehension
parse_expression_intern({bc, _, LeftElement, RightElements}, ParseState) ->
    parse_expression_intern(LeftElement, lists:foldl(fun parse_expression_intern/2, ParseState, RightElements));
% binary generator
parse_expression_intern({b_generate, _, Pattern, Expression}, ParseState) ->
    parse_expression_intern(Pattern, parse_expression_intern(Expression, ParseState));
parse_expression_intern({call, _, Fun, Parameters}, ParseState) ->
    parse_expression_intern(Parameters, parse_expression_intern(Fun, ParseState));
parse_expression_intern({'case', _, Value, Clauses}, ParseState) ->
    parse_expression_intern(Clauses, parse_expression_intern(Value, ParseState));
parse_expression_intern({clause, _Line, Pattern, _, Clause}, ParseState) ->
    parse_expression_intern(Clause, parse_expression_intern(Pattern, ParseState));
parse_expression_intern({cons, _, Head, Tail}, ParseState) ->
    parse_expression_intern(Tail, parse_expression_intern(Head, ParseState));
parse_expression_intern({'fun', _, {clauses, Clauses}}, ParseState) ->
    lists:foldl(fun parse_expression_intern/2, ParseState, Clauses);
parse_expression_intern({named_fun, _Loc, _Name, Clauses}, ParseState) -> % EEP37: Funs with names
    lists:foldl(fun parse_expression_intern/2, ParseState, Clauses);
parse_expression_intern({'fun', _, {function, _Name, _Arity}}, ParseState) ->
    ParseState;
parse_expression_intern({'fun', _, {function, _Module, _Name, _Arity}}, ParseState) ->
    ParseState;
parse_expression_intern({generate, _, Expression, L}, ParseState) ->
    parse_expression_intern(L, parse_expression_intern(Expression, ParseState));
parse_expression_intern({'if', _, Clauses}, ParseState) ->
    lists:foldl(fun parse_expression_intern/2, ParseState, Clauses);
parse_expression_intern({lc, _, Expression, Qualifiers}, ParseState) ->
    parse_expression_intern(Qualifiers, parse_expression_intern(Expression, ParseState));
parse_expression_intern({match, _, Left, Right}, ParseState) ->
    parse_expression_intern(Left, parse_expression_intern(Right, ParseState));
parse_expression_intern({op, _, _, Value}, ParseState) ->
    parse_expression_intern(Value, ParseState);
parse_expression_intern({op, _, _, Left, Right}, ParseState) ->
    parse_expression_intern(Left, parse_expression_intern(Right, ParseState));
parse_expression_intern({'receive', _, Clauses}, ParseState) ->
    lists:foldl(fun parse_expression_intern/2, ParseState, Clauses);
parse_expression_intern({'receive', _, Clauses, Timeout, AfterBody}, ParseState) ->
    ParseState2 = lists:foldl(fun parse_expression_intern/2, ParseState, Clauses),
    ParseState3 = parse_expression_intern(Timeout, ParseState2),
    parse_expression_intern(AfterBody, ParseState3);
parse_expression_intern({record, _, _Name, Fields}, ParseState) ->
    lists:foldl(fun parse_expression_intern/2, ParseState, Fields);
parse_expression_intern({record, _, _Variable, _Name, Fields}, ParseState) ->
    lists:foldl(fun parse_expression_intern/2, ParseState, Fields);
parse_expression_intern({record_field, _, Name, Value}, ParseState) ->
    parse_expression_intern(Value, parse_expression_intern(Name, ParseState));
parse_expression_intern({record_field, _, Name, _RecordType, Value}, ParseState) ->
    parse_expression_intern(Value, parse_expression_intern(Name, ParseState));
parse_expression_intern({record_index, _, _Name, Value}, ParseState) ->
    parse_expression_intern(Value, ParseState);
parse_expression_intern({'try', _, Body, CaseClauses, CatchClauses, AfterBody}, ParseState) ->
    ParseState2 = parse_expression_intern(Body, ParseState),
    ParseState3 = lists:foldl(fun parse_expression_intern/2, ParseState2, CaseClauses),
    ParseState4 = lists:foldl(fun parse_expression_intern/2, ParseState3, CatchClauses),
    parse_expression_intern(AfterBody, ParseState4);
parse_expression_intern({'catch', _, Expression}, ParseState) ->
    parse_expression_intern(Expression, ParseState);
parse_expression_intern({block, _, ExpressionList}, ParseState) ->
    lists:foldl(fun parse_expression_intern/2, ParseState, ExpressionList);
parse_expression_intern({tuple, _, Values}, ParseState) ->
    lists:foldl(fun parse_expression_intern/2, ParseState, Values);
parse_expression_intern({remote, _, Module, Fun}, ParseState) ->
    parse_expression_intern(Module, parse_expression_intern(Fun, ParseState));
parse_expression_intern({var, _, _Variable}, ParseState) ->
    %ct:pal("~w", [_Variable]),
    ParseState;
parse_expression_intern({atom, _, Atom}, ParseState) ->
    tester_parse_state:add_atom(Atom, ParseState);
parse_expression_intern({bin, _, [{bin_element,_,{string,_,String},default,default}]},
                 ParseState) ->
    tester_parse_state:add_binary(list_to_binary(String), ParseState);
parse_expression_intern({bin, _, [{bin_element,_,{var,_,_},{integer,_,_},default}]},
                 ParseState) ->
    ParseState;
parse_expression_intern({bin, _, [{bin_element,_,{var,_,_},{integer,_,_},[binary]}]},
                 ParseState) ->
    ParseState;
parse_expression_intern({bin, _, [{bin_element,_,{var,_,_},default,[binary]}]},
                 ParseState) ->
    ParseState;
parse_expression_intern({bin, _, []}, ParseState) ->
    ParseState;
parse_expression_intern({bin, _, Elements}, ParseState) when is_list(Elements) ->
    BinStrings =
        [String ||
         {bin_element,_,{string,_,String},default,default} <- Elements],
    lists:foldl(
      fun(String, Acc) ->
              tester_parse_state:add_binary(list_to_binary(String), Acc)
      end, ParseState, BinStrings);
parse_expression_intern({float, _, Float}, ParseState) ->
    tester_parse_state:add_float(Float, ParseState);
parse_expression_intern({char, _, _Char}, ParseState) ->
    % @todo
    ParseState;
parse_expression_intern({integer, _, Integer}, ParseState) ->
    tester_parse_state:add_integer(Integer, ParseState);
parse_expression_intern({map, _, L}, ParseState) when is_list(L) -> % map:new(X)
    parse_expression_intern(L, ParseState);
parse_expression_intern({map, _, Var, L}, ParseState) when is_list(L) -> % map:new(X)
    parse_expression_intern(Var, parse_expression_intern(L, ParseState));
parse_expression_intern({map_field_exact, _, Left, Right}, ParseState) -> % map field
    parse_expression_intern(Left, parse_expression_intern(Right, ParseState));
parse_expression_intern({map_field_assoc, _, Left, Right}, ParseState) -> % map field
    parse_expression_intern(Left, parse_expression_intern(Right, ParseState));
parse_expression_intern({nil, _}, ParseState) ->
    tester_parse_state:add_atom(nil, ParseState);
parse_expression_intern({string, _, String}, ParseState) ->
    tester_parse_state:add_string(String, ParseState);

parse_expression_intern(Expression, ParseState) ->
    ct:pal("unknown expression: ~w in ~w:~w", [Expression,
                                               erlang:get(module),
                                               erlang:get(fun_name)]),
    throw(unknown_expression),
    ParseState.

