%  @copyright 2017 Zuse Institute Berlin

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
%% @doc    helper module for modr mode
%% @end
-module(modr).
-author('schuett@zib.de').

-include("scalaris.hrl").

-export([is_enabled/0]).

-spec is_enabled() -> boolean().
is_enabled() ->
    config:read(key_creator) =:= modr.
