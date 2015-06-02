% @copyright 2010-2011 Zuse Institute Berlin

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
%% @doc    Common types and function specs for ring maintenance implementations.
%% @end
%% @version $Id$

-export_type([state/0, custom_message/0]).

-export([init_first/0, init/3, trigger_action/1, handle_custom_message/2,
         trigger_interval/0,
         zombie_node/2, fd_notify/4,
         new_pred/2, new_succ/2,
         update_node/2, contact_new_nodes/1,
         get_neighbors/1,
         get_web_debug_info/1,
         check_config/0,
         unittest_create_state/1]).
