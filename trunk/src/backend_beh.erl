% @copyright 2013 Zuse Institute Berlin,

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

%% @author Jan Fajerski <fajerski@zib.de>
%% @doc    Behaviour for DB back-ends.
%% @end
%% @version $Id$
-module(backend_beh).
-author('fajerski@zib.de').
-vsn('$Id$').

-type db() :: any().
-type key() :: term(). %% '$end_of_table' is not allowed as key() or else iterations won't work with ets!
-type entry() :: tuple().
-type left_bracket() :: '(' | '['.
-type right_bracket() :: ')' | ']'.
-type interval() :: {element, key()} | all | {interval, left_bracket(), key(), key(), right_bracket()}.

-ifdef(with_export_type_support).
-export_type([db/0, key/0, entry/0, interval/0]).
-endif.

% for tester:
-export([tester_is_valid_db_key/1, tester_create_db_key/1]).
-export([tester_is_valid_db_entry/1, tester_create_db_entry/1]).

-ifdef(have_callback_support).
-include("scalaris.hrl").

-callback new(DBName::nonempty_string()) -> db().
-callback open(DBName::nonempty_string()) -> db().
-callback close(db()) -> true.
-callback put(db(), entry()) -> db().
-callback get(db(), key()) -> entry() | {}.
-callback delete(db(), key()) -> db().

-callback get_name(db()) -> nonempty_string().
-callback get_load(db()) -> non_neg_integer().

-callback foldl(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
-callback foldl(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A, interval()) -> Acc1::A.
-callback foldl(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A, interval(), non_neg_integer()) -> Acc1::A.

-callback foldr(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
-callback foldr(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A, interval()) -> Acc1::A.
-callback foldr(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A, interval(), non_neg_integer()) -> Acc1::A.

-else.

-export([behaviour_info/1]).
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
        {new, 1}, {open, 1}, {close, 1}, {put, 2}, {get, 2}, {delete, 2},
        {get_name, 1}, {get_load, 1}, 
        {foldl, 3}, {foldl, 4}, {foldl, 5},
        {foldr, 3}, {foldr, 4}, {foldr, 5}
    ];
behaviour_info(_Other) ->
    undefined.

-endif.

-spec tester_is_valid_db_key(term()) -> boolean().
tester_is_valid_db_key('$end_of_table') -> false;
tester_is_valid_db_key(_) -> true.

-spec tester_create_db_key(term()) -> key().
tester_create_db_key('$end_of_table') -> end_of_table;
tester_create_db_key(K) -> K.

-spec tester_is_valid_db_entry(tuple()) -> boolean().
tester_is_valid_db_entry({}) -> false;
tester_is_valid_db_entry(E) when element(1, E) =:= '$end_of_table' -> false;
tester_is_valid_db_entry(_) -> true.

-spec tester_create_db_entry(tuple()) -> key().
tester_create_db_entry({}) -> {empty_tuple};
tester_create_db_entry(E) when element(1, E) =:= '$end_of_table' -> 
    setelement(1, E, end_of_table);
tester_create_db_entry(K) -> K.
