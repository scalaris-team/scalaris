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
%%% File    : tparticipant.erl
%%% Author  : Monika Moser <moser@zib.de>
%%% Description : transaction algorithm related to a participant
%%%               unlike for the transaction manager there won't be 
%%%                   a separate thread for each participant
%%%                   
%%%
%%% Created :  11 Feb 2008 by Monika Moser <moser@zib.de>
%%%-------------------------------------------------------------------
%% @author Monika Moser <moser@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: tparticipant.erl 463 2008-05-05 11:14:22Z schuett $

-module(transstore.tparticipant).

-author('moser@zib.de').
-vsn('$Id: tparticipant.erl 463 2008-05-05 11:14:22Z schuett $ ').

-include("trecords.hrl").
-include("../chordsharp.hrl").

-export([tp_validate/3, tp_commit/2, tp_abort/2]).

-import(?DB).
-import(gb_trees).
-import(lists).
-import(io).
-import(cs_send).



%% TP method:
%%     validate operation on a certain item
%%     only committed items can be read
tp_validate(State, Tid, Item)->
    %?TLOGN("validating item ~p", [Item]),
    %% Check whether the version is still valid
    LocalItemRes = ?DB:read(Item#item.rkey),
   
    if
	LocalItemRes /= failed -> 
	    {ok, _, Version} = LocalItemRes,
	    %?TLOGN("stored locally ~p", [Item#item.key]),
	    VCheckRes = check_version(Item, Version);
	true ->
	    Operation = Item#item.operation,
	    if 
		Operation == write ->
		    VCheckRes = success;
		true ->
		    VCheckRes = fail
	    end    
    end,
    
    %?TLOGN("version check: ~p", [VCheckRes]),

    if
	VCheckRes == success->
	    case Item#item.operation of
		read ->
		    LockRes = ?DB:set_read_lock(Item#item.rkey);
		write ->
		    LockRes = ?DB:set_write_lock(Item#item.rkey);
		_X ->
		    LockRes = failed
	    end,
	    
	    case LockRes of
		ok ->
		    %?TLOGN("lock successful for ~p", [Item#item.rkey]),
		    NewLogEntry = tp_log:new_logentry(prepared, Tid, Item),
		    NewState1 = tp_log:add_to_undecided(State, Tid, NewLogEntry),
		    Decision = prepared;
		failed ->
		    %?TLOGN("lock failed for ~p", [Item#item.rkey]), 
                    NewLogEntry = tp_log:new_logentry(local_abort, Tid, Item),
		    NewState1 = tp_log:add_to_undecided(State, Tid, NewLogEntry),
		    Decision = abort;
		_ ->
		    %% something unexpected resulted
		    Decision = abort,
		    NewState1 = State
	    end;
	    %% add to log with prepared else with abort TODO??
	true ->
	    NewLogEntry = tp_log:new_logentry(local_abort, Tid, Item),
	    NewState1 = tp_log:add_to_undecided(State, Tid, NewLogEntry),
	    Decision = abort
    end,
    NewVote = trecords:new_vote(Tid, Item#item.key, Item#item.rkey, Decision, 1),
    
    tsend:send_vote_to_rtms(Item#item.tms, NewVote),
    NewState1.
    
check_version(TransactionItem, Version)->
    
    case TransactionItem#item.operation of
	read ->
	    if
		TransactionItem#item.version >= Version ->   
		    success;
		true ->
		    fail
	    end;
	write ->
	    if
		TransactionItem#item.version == (Version + 1) ->
		    success;
		true ->
		    fail
	    end
    end.

tp_commit(State, TransactionID)->
    %?TLOGN("committing transaction ~p", [TransactionID]),
    TransLog = tp_log:get_log(State),
    TransLogUndecided = TransLog#translog.undecided,
    IsLogged = gb_trees:is_defined(TransactionID, TransLogUndecided), 
    if
	IsLogged == true ->
	    LogEntries = gb_trees:get(TransactionID, TransLogUndecided),
	    tp_commit_store_unlock(LogEntries),
	    NewTransLog = tp_log:get_log(State),
	    tp_log:remove_from_undecided(State, TransactionID, NewTransLog, TransLogUndecided);
	true ->
	    %%get information about transaction --- might have missed something before
	    State
    end.

tp_commit_store_unlock([]) ->
    ok;
tp_commit_store_unlock([Entry | LogRest])->
    if 
	Entry#logentry.operation == write ->
	    ?DB:write(Entry#logentry.rkey, Entry#logentry.value, Entry#logentry.version),
	    _Stored = ?DB:read(Entry#logentry.rkey),
	    ?DB:unset_write_lock(Entry#logentry.rkey);
	true ->
	    ?DB:unset_read_lock(Entry#logentry.rkey)
    end,
    
    tp_commit_store_unlock(LogRest).

tp_abort(State, TransactionID)->
    ?TLOGN("aborting transaction ~p", [TransactionID]),
    TransLog = tp_log:get_log(State),
    TransLogUndecided = TransLog#translog.undecided,
    IsLogged = gb_trees:is_defined(TransactionID, TransLogUndecided), 
    if
	IsLogged == true ->
	    LogEntries = gb_trees:get(TransactionID, TransLogUndecided),
	    tp_abort_unlock(LogEntries),
	    NewTransLog = tp_log:get_log(State),
	    tp_log:remove_from_undecided(State, TransactionID, NewTransLog, TransLogUndecided);
	true ->
	    %%get information about transaction --- might have missed something before
	    State
    end.

tp_abort_unlock([]) ->
    ok;	    
tp_abort_unlock([Entry | LogRest])->
     if 
	Entry#logentry.operation == write ->
	    ?DB:unset_write_lock(Entry#logentry.rkey);
	true ->
	    ?DB:unset_read_lock(Entry#logentry.rkey)
    end,
    tp_abort_unlock(LogRest).    
    
