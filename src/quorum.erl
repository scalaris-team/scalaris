% @copyright 2010-2015 Zuse Institute Berlin

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
%% @doc Service functions for handling quorums.
%% @end
%% @version $Id$
-module(quorum).
-author('schintke@zib.de').
-vsn('$Id$').

-export([majority_for_accept/1,
         majority_for_deny/1,
         minority/1]).

%% @doc Gives the majority required for a quorum to accept.
-spec majority_for_accept(integer()) -> integer().
majority_for_accept(SetSize) -> SetSize div 2 + 1.

%% @doc Gives the majority required for a quorum to deny.
-spec majority_for_deny(integer()) -> integer().
majority_for_deny(SetSize) -> SetSize div 2 + SetSize rem 2.

%% @doc Gives the minority.
-spec minority(integer()) -> integer().
minority(SetSize) -> SetSize - majority_for_accept(SetSize).
