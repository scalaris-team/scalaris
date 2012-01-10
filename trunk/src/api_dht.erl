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
%% @doc API for access to the non replicated DHT items.
%% @end
%% @version $Id$
-module(api_dht).
-author('schuett@zib.de').
-vsn('$Id$').

-export([hash_key/1]).

-include("scalaris.hrl").
-include("client_types.hrl").

-spec hash_key(client_key()) -> ?RT:key().
hash_key(Key) -> ?RT:hash_key(Key).
