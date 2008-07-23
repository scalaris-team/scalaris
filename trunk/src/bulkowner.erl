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
%%% File    : bulkowner.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : bulk owner operation TODO
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: bulkowner.erl 463 2008-05-05 11:14:22Z schuett $
-module(bulkowner).

-author('schuett@zib.de').
-vsn('$Id: bulkowner.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([issue_bulk_owner/2, start_bulk_owner/3, bulk_owner/5]).

%% @doc start a bulk owner operation.
%%      sends the message to all nodes in the given interval
%% @spec issue_bulk_owner(intervals:interval(), term()) -> ok
issue_bulk_owner(I, Msg) ->
    {ok, CSNode} = process_dictionary:find_cs_node(),
    CSNode ! {start_bulk_owner, I, Msg}.

start_bulk_owner(State, I, Msg) ->
    self() ! {bulk_owner, I, I, cs_state:me(State), Msg}.

bulk_owner(State, I, R, Next, Msg) ->
    MS = intervals:cut(R, intervals:make(cs_state:get_my_range(State))),
    IsEmpty = intervals:is_empty(MS),
    if
	not IsEmpty ->
	    self() ! msg                                                      %% deliver
    end,
    U = cs_state:rt(State),
    bulk_owner_iter(State, U, gb_trees:size(U) - 1, I, cs_state:id(State), Next, Next, false, Msg).


bulk_owner_iter(State, U, 0, I, Limit, _, Next, SentSucc, Msg) ->
    U1 = dict:fetch(1, U),
    J = intervals:new(cs_state:id(State), node:id(U1)),
    I_cut_J = intervals:cut(I, J),
    IsEmpty = intervals:is_empty(I_cut_J),
    if
	not IsEmpty and not SentSucc and not (Next == U1) ->
	    node:pidX(U1) ! {bulk_owner, intervals:empty(), I_cut_J, Limit, Msg}
    end;
    
bulk_owner_iter(State, U, Index, I, Limit, LNext, Next, SentSucc, Msg) ->
    UI   = dict:fetch(Index, U),
    UIM1 = dict:fetch(Index - 1, U),
    J = intervals:new(node:id(UI), Limit),
    I_cut_J = intervals:cut(I, J),
    IsEmpty = intervals:is_empty(I_cut_J),
    if
	not IsEmpty ->
	    K = intervals:new(node:id(UIM1), node:id(UIM1)),
	    I_cut_K = intervals:cut(I, K),
	    cs_send:send(node:pidX(UI), {bulk_own, I_cut_J, I_cut_K, LNext, Msg}),           %% deliver
	    if
		Index == 1 ->
		    bulk_owner_iter(State, U, Index - 1, I, UI, UI, Next, true, Msg);
		true ->
		    bulk_owner_iter(State, U, Index - 1, I, UI, UI, Next, SentSucc, Msg)
	    end;
	true ->
	    bulk_owner_iter(State, U, Index - 1, I, Limit, LNext, Next, SentSucc, Msg)
    end.
