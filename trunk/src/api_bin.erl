%% @copyright 2012 Zuse Institute Berlin

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
%% @doc Allows calling a function by giving a compressed (binary)
%%      {Module, Function, Parameters} tuple. The result will be a compressed
%%      binary as well.
%% @end
%% @version $Id$
-module(api_bin).
-author('kruber@zib.de').
-vsn('$Id$').

-export([apply/1]).

-spec apply(binary()) -> binary().
apply(Bin) when is_binary(Bin) ->
    {Module, Function, Parameters} = binary_to_term(Bin),
    Result = erlang:apply(Module, Function, Parameters),
    term_to_binary(Result, [{compressed, 2}, {minor_version, 1}]).
