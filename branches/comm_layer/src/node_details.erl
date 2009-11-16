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
%%% File    : node_details.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Node summary for statistics
%%%
%%% Created :  7 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(node_details).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([new/8, predlist/1, me/1, succlist/1, load/1, hostname/1, rt_size/1, message_log/1, memory/1]).
-include("../include/scalaris.hrl").
-record(node_details, {predlist, node, succlist, load, hostname, rt_size, message_log, memory}).
new(Pred, Node, SuccList, Load, Hostname, RTSize, Log, Memory) ->
    #node_details{
     predlist = Pred,
     node = Node,
     succlist = SuccList,
     load = Load,
     hostname = Hostname,
     rt_size = RTSize,
     message_log = Log,
     memory = Memory
    }.
			
predlist(#node_details{predlist=PredList}) ->   
    PredList.
me(#node_details{node=Me}) -> Me.
succlist(#node_details{succlist=SuccList}) ->
                 SuccList.
       
load(#node_details{load=Load}) -> Load.
hostname(#node_details{hostname=Hostname}) -> Hostname.
rt_size(#node_details{rt_size=RTSize}) -> RTSize.
message_log(#node_details{message_log=Log}) -> Log.
memory(#node_details{memory=Memory}) -> Memory.

    
