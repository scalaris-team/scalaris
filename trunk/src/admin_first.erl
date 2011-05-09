%  @copyright 2007-2011 Zuse Institute Berlin

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
%% @doc    determine if first vm in this deployment
%% @end
%% @version $Id: $
-module(admin_first).
-author('schuett@zib.de').
-vsn('$Id:$').

-include("scalaris.hrl").

-export([is_first_vm/0]).

-spec is_first_vm() -> boolean().
is_first_vm() ->
    util:is_unittest()
        orelse
    util:app_get_env(first, false) =:= true.
