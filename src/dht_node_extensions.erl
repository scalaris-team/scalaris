%  @copyright 2016 Zuse Institute Berlin

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
%% @doc    dht node extensions
%% @end
-module(dht_node_extensions).
-author('schintke@zib.de').
-author('schuett@zib.de').

-include("scalaris.hrl").

-export([on/2, init/1, wrap/2]).

-export_type([extension/0]).

-type extension() :: atom().

-spec wrap(extension(), comm:message()) -> {atom(), {extension(), comm:message()}}.
wrap(Extension, Message) ->
    {extensions, {Extension, Message}}.

-spec on({extensions, {extension(), comm:message()}}, dht_node_state:state()) -> dht_node_state:state().
on({extensions, {Extension, Message}}, State) ->
    Extension:on(Message, State).

-spec init(any()) -> ok.
init(Options) ->
    Extensions = config:read(extensions),
    _ = [Extension:init(Options) || Extension <- Extensions],
    ok.
