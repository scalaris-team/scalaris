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

%% @author Thorsten Schuett <schuett@zib.de>
%% @author Florian Schintke <schintke@zib.de>
%% @doc    data_node join protocol
%% @end
%% @version $Id$
-module(data_node_join).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id').
-include("scalaris.hrl").

-export([join_as_first/2]).

-spec join_as_first(data_node:state(), [tuple()]) -> data_node:state().
join_as_first(State, _Options) ->
    Lease = leases:new_for_all(),
    LMState = data_node:get_rlease_mgmt(State),
    LMState1 = rlease_mgmt:store_rlease(LMState, Lease, 0),
    LMState2 = rlease_mgmt:store_rlease(LMState1, Lease, 1),
    LMState3 = rlease_mgmt:store_rlease(LMState2, Lease, 2),
    LMState4 = rlease_mgmt:store_rlease(LMState3, Lease, 3),
    State1 = data_node:set_leases(State, [Lease]),
    data_node:set_rlease_mgmt(State1, LMState4).
