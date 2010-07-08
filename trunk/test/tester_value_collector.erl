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
%%% Description : value collector for test generator
%%%
%%% Created :  30 April 2010 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(tester_value_collector).

-author('schuett@zib.de').
-vsn('$Id$').

-include_lib("unittest.hrl").

-export([parse_expression/2]).

parse_expression(Clauses, ParseState) when is_list(Clauses) ->
    lists:foldl(fun parse_expression/2, ParseState, Clauses);
parse_expression({call, _, Fun, Parameters}, ParseState) ->
    parse_expression(Parameters, parse_expression(Fun, ParseState));
parse_expression({'case', _, Value, Clauses}, ParseState) ->
    parse_expression(Clauses, parse_expression(Value, ParseState));
parse_expression({clause, _, _, _, Clause}, ParseState) ->
    parse_expression(Clause, ParseState);
parse_expression({cons, _, Head, Tail}, ParseState) ->
    parse_expression(Tail, parse_expression(Head, ParseState));
parse_expression({'fun', _, {clauses, Clauses}}, ParseState) ->
    lists:foldl(fun parse_expression/2, ParseState, Clauses);
parse_expression({'fun', _, {function, _Name, _Arity}}, ParseState) ->
    ParseState;
parse_expression({'fun', _, {function, _Module, _Name, _Arity}}, ParseState) ->
    ParseState;
parse_expression({generate, _, Expression, L}, ParseState) ->
    parse_expression(L, parse_expression(Expression, ParseState));
parse_expression({'if', _, Clauses}, ParseState) ->
    lists:foldl(fun parse_expression/2, ParseState, Clauses);
parse_expression({lc, _, Expression, Qualifiers}, ParseState) ->
    parse_expression(Qualifiers, parse_expression(Expression, ParseState));
parse_expression({match, _, Left, Right}, ParseState) ->
    parse_expression(Left, parse_expression(Right, ParseState));
parse_expression({op, _, _, Value}, ParseState) ->
    parse_expression(Value, ParseState);
parse_expression({op, _, _, Left, Right}, ParseState) ->
    parse_expression(Left, parse_expression(Right, ParseState));
parse_expression({'receive', _, Clauses}, ParseState) ->
    lists:foldl(fun parse_expression/2, ParseState, Clauses);
parse_expression({'receive', _, Clauses, Timeout, AfterBody}, ParseState) ->
    ParseState2 = lists:foldl(fun parse_expression/2, ParseState, Clauses),
    ParseState3 = parse_expression(Timeout, ParseState2),
    parse_expression(AfterBody, ParseState3);
parse_expression({record, _, _Name, Fields}, ParseState) ->
    lists:foldl(fun parse_expression/2, ParseState, Fields);
parse_expression({record, _, _Variable, _Name, Fields}, ParseState) ->
    lists:foldl(fun parse_expression/2, ParseState, Fields);
parse_expression({record_field, _, Name, Value}, ParseState) ->
    parse_expression(Value, parse_expression(Name, ParseState));
parse_expression({record_field, _, Name, _RecordType, Value}, ParseState) ->
    parse_expression(Value, parse_expression(Name, ParseState));
parse_expression({'try', _, Body, CaseClauses, CatchClauses, AfterBody}, ParseState) ->
    ParseState2 = parse_expression(Body, ParseState),
    ParseState3 = lists:foldl(fun parse_expression/2, ParseState2, CaseClauses),
    ParseState4 = lists:foldl(fun parse_expression/2, ParseState3, CatchClauses),
    parse_expression(AfterBody, ParseState4);
parse_expression({'catch', _, Expression}, ParseState) ->
    parse_expression(Expression, ParseState);
parse_expression({block, _, ExpressionList}, ParseState) ->
    lists:foldl(fun parse_expression/2, ParseState, ExpressionList);
parse_expression({tuple, _, Values}, ParseState) ->
    lists:foldl(fun parse_expression/2, ParseState, Values);
parse_expression({remote, _, Module, Fun}, ParseState) ->
    parse_expression(Module, parse_expression(Fun, ParseState));
parse_expression({var, _, _Variable}, ParseState) ->
    ParseState;

parse_expression({atom, _, Atom}, ParseState) ->
    tester_parse_state:add_atom(Atom, ParseState);
parse_expression({bin, _, _Binary}, ParseState) ->
    % @todo
    ParseState;
parse_expression({float, _, _Float}, ParseState) ->
    % @todo
    ParseState;
parse_expression({char, _, _Char}, ParseState) ->
    % @todo
    ParseState;
parse_expression({integer, _, Integer}, ParseState) ->
    tester_parse_state:add_integer(Integer, ParseState);
parse_expression({nil, _}, ParseState) ->
    tester_parse_state:add_atom(nil, ParseState);
parse_expression({string, _, String}, ParseState) ->
    tester_parse_state:add_string(String, ParseState);

parse_expression(Expression, _ParseState) ->
    ?ct_fail("unknown expression: ~w", [Expression]).

