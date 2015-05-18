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
-module(db_backend_beh).
-author('fajerski@zib.de').
-vsn('$Id$').

-type db() :: any().
-type key() :: term(). %% '$end_of_table' is not allowed as key() or else iterations won't work with ets!
-type entry() :: {key(), term()}
               | {key(), term(), term()}
               | {key(), term(), term(), term()}
               | {key(), term(), term(), term(), term()}
               | {key(), term(), term(), term(), term(), term()}
               | {key(), term(), term(), term(), term(), term(), term()}
               | {key(), term(), term(), term(), term(), term(), term(), term()}
               | {key(), term(), term(), term(), term(), term(), term(), term(), term()}
               | {key(), term(), term(), term(), term(), term(), term(), term(), term(), term()}
               | {key(), term(), term(), term(), term(), term(), term(), term(), term(), term(), term()}.
-type left_bracket() :: '(' | '['.
-type right_bracket() :: ')' | ']'.
-type interval() :: {key()} | all | {left_bracket(), key(), key(), right_bracket()}.

-export_type([db/0, key/0, entry/0, interval/0]).

% for tester:
-export([tester_is_valid_db_key/1, tester_create_db_key/1]).

-ifdef(have_callback_support).
-include("scalaris.hrl").

-callback new(DBName::nonempty_string()) -> db().
-callback open(DBName::nonempty_string()) -> db().
-callback close(db()) -> true.
-callback close_and_delete(db()) -> true.
-callback put(db(), entry()) -> db().
-callback get(db(), key()) -> entry() | {}.
-callback delete(db(), key()) -> db().

-callback get_persisted_tables() -> [nonempty_string()].
-callback get_name(db()) -> nonempty_string().
-callback get_load(db()) -> non_neg_integer().
-callback is_available() -> boolean() | [MissingModule::atom()].
-callback supports_feature(Feature::atom()) -> boolean().

-callback foldl(db(), fun((key(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
-callback foldl(db(), fun((key(), AccIn::A) -> AccOut::A), Acc0::A, interval()) -> Acc1::A.
-callback foldl(db(), fun((key(), AccIn::A) -> AccOut::A), Acc0::A, interval(), non_neg_integer()) -> Acc1::A.

-callback foldr(db(), fun((key(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
-callback foldr(db(), fun((key(), AccIn::A) -> AccOut::A), Acc0::A, interval()) -> Acc1::A.
-callback foldr(db(), fun((key(), AccIn::A) -> AccOut::A), Acc0::A, interval(), non_neg_integer()) -> Acc1::A.

-callback foldl_unordered(DB::db(), Fun::fun((Entry::entry(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.

-callback tab2list(Table_name::db()) -> [Entries::entry()].
-else.

-export([behaviour_info/1]).
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
        {new, 1}, {open, 1}, {close, 1}, {close_and_delete, 1}, {put, 2}, {get, 2}, {delete, 2},
        {get_persisted_tables, 0}, {get_name, 1}, {get_load, 1}, 
        {supports_feature, 1}, {is_available, 0},
        {foldl, 3}, {foldl, 4}, {foldl, 5},
        {foldr, 3}, {foldr, 4}, {foldr, 5},
        {foldl_unordered, 3},
        {tab2list, 1}
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
