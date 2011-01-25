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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Replicated-state machine behavior
%% @end
%% @version $Id$
-module(rsm_beh).
-author('schuett@zib.de').
-vsn('$Id$').

-export([behaviour_info/1]).

% init_state() must be serializable, i.e. don't put ets-tables in here

-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {init_state, 0},
     {on, 2}
    ];

behaviour_info(_Other) ->
    undefined.
