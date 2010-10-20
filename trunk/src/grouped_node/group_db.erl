%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @version $Id$
-module(group_db).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([new_empty/0, new_replica/0,
        write/4, read/2]).

-type(mode_type() :: is_current | is_filling).

-type(state() :: {Mode::mode_type(), DB::?DB:db()}).

-ifdef(with_export_type_support).
-export_type([state/0]).
-endif.

-spec new_empty() -> {is_current, ?DB:db()}.
new_empty() ->
    % @todo
    {is_current, ?DB:new(?RT:hash_key(1))}.

-spec new_replica() -> {is_filling, ?DB:db()}.
new_replica() ->
    % @todo
    {is_filling, ?DB:new(?RT:hash_key(1))}.

-spec write(state(), ?RT:key(), any(), non_neg_integer()) ->
    state().
write({is_current, DB}, Key, Value, Version) ->
    {is_current, ?DB:write(DB, Key, Value, Version)}.

-spec read(state(), ?RT:key()) -> {value, any()} | is_not_current.
read({is_current, DB}, HashedKey) ->
    {value, ?DB:read(DB, HashedKey)};
read({is_filling, _DB}, _Hashed_Key) ->
    is_not_current.
