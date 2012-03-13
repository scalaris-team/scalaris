%  @copyright 2007-2012 Zuse Institute Berlin

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
%% @doc    data_node client/test file
%% @end
%% @version $Id$
-module(data_node_client).
-author('schuett@zib.de').
-vsn('$Id').

-include("scalaris.hrl").

-export([test/0]).

test() ->
    DataNode = pid_groups:find_a(data_node),
    comm:send_local(DataNode, {lookup_fin, 42, {read, comm:this(), 5, 42}}),
    receive
        X ->
            io:format("~p~n", [X])
    end.
