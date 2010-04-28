%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
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
%%% File    node.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    node data structure + functions
%%% @end
%%% Created : 3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(node).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([id/1, pidX/1,
         new/2,
         null/0, is_valid/1]).

-include("scalaris.hrl").

-record(node, {pid :: cs_send:mypid(), id :: ?RT:key()}).
-type(node_type() :: #node{}).

-spec(new/2 :: (cs_send:mypid(), ?RT:key()) -> node_type()).
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

%% @doc Checks whether the given parameter is a valid node.
-spec is_valid(node_type()) -> true;
               (null | unknown) -> false.
is_valid(X) when is_record(X, node) ->
    true;
is_valid(_) ->
    false.
