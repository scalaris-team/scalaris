%  @copyright 2012 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    data_node database file and operations
%% @end
%% @version $Id$
-module(data_node_db).
-author('schintke@zib.de').
-author('schuett@zib.de').
-vsn('$Id').
-include("scalaris.hrl").

%% operations provided for the data_node
-export([read/2]).

%% part of data_node on-handler for DB access
-export([on/2]).
-type msg() :: {read, comm:mypid(), _SourceId :: any(), ?RT:key()}.

%% operations on data_node_leases state
-export([new_state/0]).

-ifdef(with_export_type_support).
-export_type([msg/0, state/0]).
-endif.

-record(state, {db :: ?DB:db()}).
-opaque state() :: #state{}.


-spec new_state() -> state().
new_state() ->
    DBTable = ?DB:new(),
    #state{db=DBTable}.

% @todo
-spec read(state(), ?RT:key()) -> unknown | any().
read(#state{db=DBTable}, HashedKey) ->
    ?DB:read(DBTable, HashedKey).

% @todo
% - ets -> dets

-spec on(msg(), data_node:state()) -> data_node:state().
on({read, SourcePid, SourceId, HashedKey}, State) ->
    Value = read(data_node:get_db(State), HashedKey),
    Msg = {read_reply, SourceId, HashedKey, Value},
    comm:send(SourcePid, Msg),
    State.
