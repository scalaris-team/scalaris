%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(node).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([id/1, pidX/1,
         new/2,
         is_null/1, null/0]).

-include("../include/scalaris.hrl").

-record(node, {pid :: cs_send:mypid(), id :: ?RT:key()}).
-type(node_type() :: #node{}).

-spec(new/2 :: (cs_send:mypid(), any()) -> node_type()).
new(PID, Id) ->
    #node{
     pid = PID,
     id = Id}.

null() ->
    null.

-spec(pidX/1 :: (node_type()) -> cs_send:mypid()).
pidX(#node{pid=PID}) ->
    PID.

-spec(id/1 :: (node_type()) -> ?RT:key()).
id(#node{id=Id}) ->
    Id.

-spec(is_null/1 :: (null) -> true
                  ; (node_type()) -> false).
is_null(null) ->
    true;
is_null(#node{id=_}) ->
    false.
