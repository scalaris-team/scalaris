%  Copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : conn_queue.erl
%%% Author  : Thorsten Schuett <schuett@csr-pc11.zib.de>
%%% Description : Connection Queue
%%%
%%% Created : 05 Feb 2008 by Thorsten Schuett <schuett@csr-pc11.zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id $
-module(comm_layer.conn_queue).

-author('schuett@zib.de').
-vsn('$Id$ ').

%% API
-export([new/0, size/1, pop/1, push/2, delete/2, is_member/2]).

-import(gb_trees).


%% @doc creates a new connection queue.
%%  a connection queue is defined as follows
%%      {priority -> connection, connection -> priority, size, nextpriority}
%% @type conn_queue() = {gb_trees:gb_tree(), gb_trees:gb_tree(), int(), int()}
%% @spec new() -> conn_queue()
new() ->
    {gb_trees:empty(), gb_trees:empty(), 0, 0}.

%% @doc returns the size of the queue
%% @spec size(conn_queue()) -> int()
size({_Queue, _Obj2Index, Size, _NextPriority}) ->
    Size.
    
%% @doc returns and remove the first element of the queue
%% @spec pop(conn_queue()) -> {any(), conn_queue()}
pop({Index2Obj, Obj2Index, Size, NextPriority}) ->
    {_Priority, Connection, NewIndex2Obj} = gb_trees:take_smallest(Index2Obj),
    NewQueue = {NewIndex2Obj, gb_trees:delete(Connection, Obj2Index), Size - 1, NextPriority},
    {Connection, NewQueue}.

%% @doc adds a new element to the back of the queue
%% @spec push(any(), conn_queue()) -> conn_queue()
push(Connection, {Index2Obj, Obj2Index, Size, NextPriority}) ->
    {gb_trees:insert(NextPriority, Connection, Index2Obj), 
     gb_trees:insert(Connection, NextPriority, Obj2Index), 
     Size + 1, 
     NextPriority + 1}.

%% @doc removes an element from the queue
%% @spec delete(any(), conn_queue()) -> conn_queue()
delete(Connection, {Index2Obj, Obj2Index, Size, NextPriority}) ->
    Priority = gb_trees:get(Connection, Obj2Index),
    {gb_trees:delete(Priority, Index2Obj), 
     gb_trees:delete(Connection, Obj2Index), 
     Size - 1, 
     NextPriority}.
    

%% @doc checks whether a connection is known
%% @spec is_member(any(), conn_queue()) -> conn_queue()
is_member(Connection, {_Index2Obj, Obj2Index, _Size, _NextPriority}) ->
    gb_trees:is_defined(Connection, Obj2Index).
