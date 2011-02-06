%  @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc Ring maintenance behaviour
%% @end
%% @version $Id$
-module(rm_beh).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([behaviour_info/1]).

-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {init, 4},
     {on, 3},
     {zombie_node, 3},
     {crashed_node, 3},
     {new_pred, 3},
     {new_succ, 3},
     {leave, 2},
     {remove_pred, 4},
     {remove_succ, 4},
     {updated_node, 4},
     {get_web_debug_info, 2},
     {check_config, 0}
    ];

behaviour_info(_Other) ->
    undefined.
