% @copyright 2014 Zuse Institute Berlin

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
%% @doc Make configure options and findings available to the runtime.
%% @version $Id:$
-module(configure).
-author('schintke@zib.de').
-vsn('$Id:$').

-export([is_enable_debug/0,
         get_proplist/0,
         show_config/0]).

-spec is_enable_debug() -> boolean().
%% return true if './configure --enable-debug' was used.
-ifdef(enable_debug).
is_enable_debug() -> true.
-else.
is_enable_debug() -> false.
-endif.

-spec get_proplist() -> list().
get_proplist() ->
    [{enable_debug, is_enable_debug()}].

-spec show_config() -> ok.
show_config() ->
    io:format("--enable-debug: ~p~n", [is_enable_debug()]),
    ok.



