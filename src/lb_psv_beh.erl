%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc    Passive load balancing algorithm behavior
%% @end
%% @version $Id$
-module(lb_psv_beh).
-author('kruber@zib.de').
-vsn('$Id$ ').

% for behaviour
-export([behaviour_info/1]).

-include("scalaris.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Behaviour definition
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_beh:behaviour
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     % at an existing node, simulate a join operation
     {create_join, 2},
     % sort a list of candidates so that the best ones are at the front of the list 
     {sort_candidates, 1},
     % common methods
     {check_config, 0}
    ];
%% userdevguide-end rt_beh:behaviour

behaviour_info(_Other) ->
    undefined.
