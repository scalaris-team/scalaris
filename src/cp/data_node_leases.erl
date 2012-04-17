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
%% @doc    data_node lease management
%% @end
%% @version $Id$
-module(data_node_leases).
-author('schintke@zib.de').
-author('schuett@zib.de').
-vsn('$Id').
-include("scalaris.hrl").

%% part of data_node on-handler for lease management
-export([on/2]).
-type msg() :: {lease, {renew_lease}}.

%% operations on data_node_leases state
-export([new_state/0,     %% create new state
         is_owner/2       %% am I owner of a lease for given key?
        ]).

-type state() :: leases:leases().

-ifdef(with_export_type_support).
-export_type([msg/0, state/0]).
-endif.

-spec new_state() -> state().
new_state() -> [].

-spec is_owner(data_node:state(), ?RT:key()) -> boolean().
is_owner(State, Key) ->
    Leases = data_node:get_leases(State),
    leases:is_owner(Leases, Key).

-spec on(msg(), data_node:state()) -> data_node:state().
on({lease, {renew_lease}}, State) ->
    State.
