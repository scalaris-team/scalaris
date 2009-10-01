%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%
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
%%%----------------------------------------------------------------------
%%% File    : randoms.erl
%%% Author  : Thorsten Schuett
%%% Purpose :
%%% Created : 13 Dec 2002 by Alexey Shchepin <alexey@sevcom.net>
%%% Id      : $Id$
%%%----------------------------------------------------------------------

%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2002-2007 Alexey Shchepin
%% @version $Id$
-module(randoms).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("chordsharp.hrl").

-export([getRandomId/0]).

%% @doc generates a random id
%% @spec getRandomId() -> list()
getRandomId() ->
    integer_to_list(crypto:rand_uniform(1, 65536 * 65536)).


