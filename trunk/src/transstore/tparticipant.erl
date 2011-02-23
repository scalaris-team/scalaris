% @copyright 2008-2011 Zuse Institute Berlin

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

%% @author Monika Moser <moser@zib.de>
%% @doc transaction algorithm related to a participant
%%      unlike for the transaction manager there won't be 
%%      a separate thread for each participant
%% @end
%% @version $Id$
-module(tparticipant).
-author('moser@zib.de').
-vsn('$Id$').

-include("trecords.hrl").
-include("scalaris.hrl").

-export([tp_validate/3, tp_commit/2, tp_abort/2]).

%% TP method:
%%     validate operation on a certain item
%%     only committed items can be read
-spec tp_validate(dht_node_state:state(), Tid::any(), #item{}) -> dht_node_state:state().
tp_validate(State, Tid, Item)->
    %?TLOGN("validating item ~p", [Item]),
    %% Check whether the version is still valid
    StateDB = dht_node_state:get(State, db),
    {DB, LockRes} = case ?DB:read(StateDB, Item#item.rkey) of
			 {ok, _Value, Version} ->
			     set_lock(StateDB, check_version(Item, Version), Item)
%% unreachable due to dialyzer
%% 			 _Any ->
%% 			     case Item#item.operation of
%% 				 write ->
%% 				     set_lock(StateDB, success, Item);
%% 				 _Any2 ->
%% 				     {StateDB, failed}
%% 			     end
		     end,
    Decision = decision(LockRes),
    NewState = update_transaction_participant_log(dht_node_state:set_db(State, DB), Tid, Item, Decision),
    NewVote = trecords:new_vote(Tid, Item#item.key, Item#item.rkey, Decision, 1),   
    _ = tsend:send_vote_to_rtms(Item#item.tms, NewVote),
    NewState.

-spec set_lock(?DB:db(), success | fail, #item{}) -> {?DB:db(), ok | failed}.
set_lock(DB, success, #item{operation=read} = Item) ->
    ?DB:set_read_lock(DB, Item#item.rkey);
set_lock(DB, success, #item{operation=write} = Item) ->
    ?DB:set_write_lock(DB, Item#item.rkey);
set_lock(DB, _LockFailed, _Item)->
    {DB, failed}.

-spec decision(ok) -> prepared;
              (failed) -> abort.
decision(ok) -> prepared;
decision(_) -> abort.

-spec update_transaction_participant_log(State::dht_node_state:state(), Tid::any(), Item::#item{}, prepared | abort) -> dht_node_state:state().
update_transaction_participant_log(State, Tid, Item, prepared) ->
    update_state(State, Tid, tp_log:new_logentry(prepared, Tid, Item));
update_transaction_participant_log(State, Tid, Item, abort) ->
    update_state(State, Tid, tp_log:new_logentry(local_abort, Tid, Item)).

-spec update_state(OldState::dht_node_state:state(), Tid::any(), LogEntry::#logentry{}) -> dht_node_state:state().
update_state(OldState, Tid, LogEntry)->
    tp_log:add_to_undecided(OldState, Tid, LogEntry).

-spec check_version(#item{}, Version::?DB:version()) -> success | fail.
check_version(#item{operation=read} = TransactionItem, Version) ->
    if
        TransactionItem#item.version >= Version -> success;
        true -> fail
    end;
check_version(#item{operation=write} = TransactionItem, Version) ->
    if
        TransactionItem#item.version =:= (Version + 1) -> success;
        true -> fail
    end.

-spec tp_commit(State::dht_node_state:state(), TransactionID::any()) -> dht_node_state:state().
tp_commit(State, TransactionID)->
    %?TLOGN("committing transaction ~p", [TransactionID]),
    TransLog = tp_log:get_log(State),
    TransLogUndecided = TransLog#translog.undecided, 
    case gb_trees:lookup(TransactionID, TransLogUndecided) of
	{value, LogEntries} ->
	    %LogEntries = gb_trees:get(TransactionID, TransLogUndecided),
	    DB = tp_commit_store_unlock(dht_node_state:get(State, db), LogEntries),
	    State2 = dht_node_state:set_db(State, DB),
	    NewTransLog = tp_log:get_log(State2),
	    tp_log:remove_from_undecided(State2, TransactionID, NewTransLog, TransLogUndecided);
	none ->
	    %%get information about transaction --- might have missed something before
	    State
    end.

-spec tp_commit_store_unlock(DB::?DB:db(), [#logentry{}]) -> ?DB:db().
tp_commit_store_unlock(DB, []) ->
    DB;
tp_commit_store_unlock(DB, [Entry | LogRest])->
    {DB3, _} = case Entry#logentry.operation of
	write ->
	    DB2 = ?DB:write(DB, Entry#logentry.rkey, Entry#logentry.value, Entry#logentry.version),
	    %_Stored = ?DB:read(Entry#logentry.rkey),
	    ?DB:unset_write_lock(DB2, Entry#logentry.rkey);
	_Any ->
	    ?DB:unset_read_lock(DB, Entry#logentry.rkey)
    end,
    tp_commit_store_unlock(DB3, LogRest).

-spec tp_abort(State::dht_node_state:state(), TransactionID::any()) -> dht_node_state:state().
tp_abort(State, TransactionID)->
    ?TLOGN("aborting transaction ~p", [TransactionID]),
    TransLog = tp_log:get_log(State),
    TransLogUndecided = TransLog#translog.undecided,
    case gb_trees:lookup(TransactionID, TransLogUndecided) of
	{value, LogEntries} ->
	    LogEntries = gb_trees:get(TransactionID, TransLogUndecided),
	    DB = tp_abort_unlock(dht_node_state:get(State, db), LogEntries),
	    State2 = dht_node_state:set_db(State, DB),
	    NewTransLog = tp_log:get_log(State2),
	    tp_log:remove_from_undecided(State2, TransactionID, NewTransLog, TransLogUndecided);
	none ->
	    %%get information about transaction --- might have missed something before
	    State
    end.

-spec tp_abort_unlock(DB::?DB:db(), [#logentry{}]) -> ?DB:db().
tp_abort_unlock(DB, []) ->
    DB;	    
tp_abort_unlock(DB, [Entry | LogRest])->
    {DB2, _} = case Entry#logentry.status of
	prepared ->
	    case Entry#logentry.operation of
		write ->
		    ?DB:unset_write_lock(DB, Entry#logentry.rkey);
		read ->
		    ?DB:unset_read_lock(DB, Entry#logentry.rkey)
	    end;
	_Any ->
	    {DB, ok}
    end,
    tp_abort_unlock(DB2, LogRest).    
