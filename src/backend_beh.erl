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

-ifdef(have_callback_support).
-include("scalaris.hrl").

-type(db() :: any()).

-callback new(nonempty_string()) -> db().
-callback close(db()) -> true.
-callback put(db(), any()) -> db().
-callback get(db(), ?RT:key()) -> any().
-callback delete(db(), ?RT:key()) -> db().

-callback get_name(db()) -> nonempty_string().
-callback get_load(db()) -> integer().

-callback foldl(db(), fun(), any()) -> any().
-callback foldl(db(), fun(), any(), intervals:simple_interval()) -> any().
-callback foldl(db(), fun(), any(), intervals:simple_interval(), non_neg_integer()) -> any().

-callback foldr(db(), fun(), any()) -> any().
-callback foldr(db(), fun(), any(), intervals:simple_interval()) -> any().
-callback foldr(db(), fun(), any(), intervals:simple_interval(), non_neg_integer()) -> any().

-else.

-export([behaviour_info/1]).
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
        {new, 1}, {close, 1}, {put, 2}, {get, 2}, {delete, 2},
        {get_name, 1}, {get_load, 1}, 
        {foldl, 3}, {foldl, 4}, {foldl, 5},
        {foldr, 3}, {foldr, 4}, {foldr, 5}
    ];
behaviour_info(_Other) ->
    undefined.

-endif.
