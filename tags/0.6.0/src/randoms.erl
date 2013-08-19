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
%% @doc Helper functions to create random numbers.
%% @end
%% @version $Id$
-module(randoms).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([start/0, stop/0, getRandomString/0, getRandomInt/0, rand_uniform/2]).

%% @doc Starts the crypto module's server.
-spec start() -> ok.
start() -> crypto:start().

%% @doc Generates a random string in the range 1 =&lt; Id &lt; 2^32
-spec getRandomString() -> string().
getRandomString() ->
    integer_to_list(getRandomInt()).

%% @doc Generates a random integer in the range 1 =&lt; Id &lt; 2^32
-spec getRandomInt() -> pos_integer().
getRandomInt() ->
    rand_uniform(1, 16#100000000).

%% @doc Generates a random number N between Lo &lt;= N &lt; Hi using the crypto
%%      library pseudo-random number generator.
-spec rand_uniform(Lo::integer(), Hi::integer()) -> integer().
rand_uniform(Lo, Hi) when Lo < Hi ->
    crypto:rand_uniform(Lo, Hi);
rand_uniform(Hi, Lo) when Lo < Hi ->
    crypto:rand_uniform(Lo, Hi);
rand_uniform(X, X) -> X.

%% @doc Stops the crypto module's server.
-spec stop() -> ok.
stop() -> crypto:stop().
