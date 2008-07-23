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
%%%-------------------------------------------------------------------
%%% File    : node.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : node data structure + functions
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: node.erl 463 2008-05-05 11:14:22Z schuett $
-module(node).

-author('schuett@zib.de').
-vsn('$Id: node.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([id/1, pidX/1, 
	 new/3, make/2,
	 uniqueId/1,
	 is_null/1, null/0]).

-record(node, {pid, id, uniqueId}).

new(PID, Id, UniqueId) ->
    #node{
     pid = PID, 
     id = Id,
    uniqueId = UniqueId}.

make(PID, Id) ->
    UniqueId = randoms:getRandomId(),
    new(PID, Id, UniqueId).

null() ->
    null.

uniqueId(#node{uniqueId=A}) ->
    A.
    
pidX(#node{pid=PID}) ->
    PID.

id(#node{id=Id}) ->
    Id.

is_null(null) ->
    true;
is_null(#node{id=_}) ->
    false.
