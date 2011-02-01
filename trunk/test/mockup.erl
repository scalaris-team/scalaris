%  @copyright 2011 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Useful methods for mockup modules, e.g. matching of messages.
%% @end
%% @version $Id$
-module(mockup).
-author('kruber@zib.de').
-vsn('$Id$ ').

-include("scalaris.hrl").

-export([match_any/2]).

-ifdef(with_export_type_support).
-export_type([match_spec/0]).
-endif.

-type match_variable() ::
    '$1' | '$2' | '$3' | '$4' | '$5' | '$6' | '$7' | '$8' | '$9'.
-type match_head_part() ::  '_' | match_variable() |term().
-type match_head() ::
    '_' |
    % note: currently only defined up to 9-tuples
    {match_head_part()} |
    {match_head_part(), match_head_part()} |
    {match_head_part(), match_head_part(), match_head_part()} |
    {match_head_part(), match_head_part(), match_head_part(), match_head_part()} |
    {match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part()} |
    {match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part()} |
    {match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part()} |
    {match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part()} |
    {match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part(), match_head_part()}.
-type match_condition() :: none().
-type match_counter() :: pos_integer() | infinity.
-type match_spec() :: {Head::match_head(), Conditions::[match_condition()], Count::match_counter()}.

-spec match_any(Msg::comm:message(), MatchSpecs::[match_spec()]) -> {boolean(), NewMatchSpecs::[match_spec()]}.
match_any(Msg, MatchSpecs) ->
    match_any(Msg, MatchSpecs, []).

-spec match_any(Msg::comm:message(), MatchSpecs::[match_spec()], ProcessedMatchSpecs::[match_spec()]) -> {boolean(), NewMatchSpecs::[match_spec()]}.
match_any(_Msg, [], ProcessedMatchSpecs) ->
    {false, lists:reverse(ProcessedMatchSpecs)};
match_any(Msg, MatchSpecs = [First = {Head, Conditions, Count} | Rest], ProcessedMatchSpecs) ->
    case match(Msg, First) of
        false ->
            match_any(Msg, Rest, [First | ProcessedMatchSpecs]);
        true when Count =:= infinity ->
            {true, lists:append(ProcessedMatchSpecs, MatchSpecs)};
        true when (Count - 1) > 0 ->
            {true, lists:append([ProcessedMatchSpecs, [{Head, Conditions, Count - 1}], Rest])};
        true ->
            {true, lists:append([ProcessedMatchSpecs, Rest])}
    end.

-spec match(Msg::comm:message(), match_spec()) -> boolean().
match(_Msg, {'_', _Conditions = [], _Count}) ->
    true;
match(Msg, {Head, _Conditions = [], _Count}) when tuple_size(Msg) =/= tuple_size(Head) ->
    false;
match(Msg, {Head, _Conditions = [], _Count}) ->
    lists:all(fun(X) ->
                      match_head(Msg, erlang:element(X, Msg), erlang:element(X, Head))
              end, lists:seq(1, tuple_size(Msg))).

match_head(_Msg, _MsgElem, '_') -> true;
match_head(Msg, MsgElem, '$1') -> MsgElem =:= erlang:element(1, Msg);
match_head(Msg, _MsgElem, '$2') when tuple_size(Msg) < 2 -> false;
match_head(Msg, MsgElem, '$2') -> MsgElem =:= erlang:element(2, Msg);
match_head(Msg, _MsgElem, '$3') when tuple_size(Msg) < 3 -> false;
match_head(Msg, MsgElem, '$3') -> MsgElem =:= erlang:element(3, Msg);
match_head(Msg, _MsgElem, '$4') when tuple_size(Msg) < 4 -> false;
match_head(Msg, MsgElem, '$4') -> MsgElem =:= erlang:element(4, Msg);
match_head(Msg, _MsgElem, '$5') when tuple_size(Msg) < 5 -> false;
match_head(Msg, MsgElem, '$5') -> MsgElem =:= erlang:element(5, Msg);
match_head(Msg, _MsgElem, '$6') when tuple_size(Msg) < 6 -> false;
match_head(Msg, MsgElem, '$6') -> MsgElem =:= erlang:element(6, Msg);
match_head(Msg, _MsgElem, '$7') when tuple_size(Msg) < 7 -> false;
match_head(Msg, MsgElem, '$7') -> MsgElem =:= erlang:element(7, Msg);
match_head(Msg, _MsgElem, '$8') when tuple_size(Msg) < 8 -> false;
match_head(Msg, MsgElem, '$8') -> MsgElem =:= erlang:element(8, Msg);
match_head(Msg, _MsgElem, '$9') when tuple_size(Msg) < 9 -> false;
match_head(Msg, MsgElem, '$9') -> MsgElem =:= erlang:element(9, Msg);
match_head(_Msg, MsgElem, HeadElem) -> MsgElem =:= HeadElem.
