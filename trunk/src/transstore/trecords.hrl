%  Copyright 2007-2011 Zuse Institute Berlin
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
%%% File    : trecords.hrl
%%% Author  : Monika Moser <moser@zib.de>
%%% Description : contains records used by several modules
%%%
%%% Created :  17 Jul 2007 by Monika Moser <moser@zib.de>
%%%-------------------------------------------------------------------
%% @author Monika Moser <moser@zib.de>
%% @copyright 2007-2011 Zuse Institute Berlin
%% @version $Id$

%% the remotly sent one
-record(item, {key, rkey, value, version, operation, tms}).

%% @type translog() = {gb_trees:gb_tree(), gb_trees:gb_tree()}.
% the information about items involved in a transaction
-record(translog, {tid_tm_mapping, undecided, decided}).

%% the entry for a transaction per item stored in the log
%% contains information about the item and the transaction
%% status: prepared/local_abort/commit/abort
-record(logentry, {status, key, rkey, value, version, operation,
                   transactionID, tms}).

%% the vote of a participant
-record(vote, {transactionID, key, rkey, decision, timestamp}).

%% the message sent to tps
-record(tp_message, {item_key, orig_key, message}).

%% the message sent to rtms
-record(tm_message, {transaction_id, tm_key, message}).

%% the state of a transaction manager
-record(tm_state, {transID,  items, numitems, leader,  myBallot, rtms,
                   votes, vote_acks, read_vote_acks, decisions,
                   rtms_found, tps_found, status, decision}).

%% information on an item hold by a transaction manager
-record(tm_item, {key, value, version, operation, tps, tps_found}).

%-define(TLOG(X), io:format("TRANSACTION ~p:~p ~p~n" ,[?MODULE, ?LINE, X])).
%-define(TLOG2(X, Y), io:format("TRANSACTION ~p:~p ~p ~p ~n" ,[?MODULE, ?LINE, X, Y])).

-define(TLOG(X), ok).
-define(TLOG2(X, Y), ok).

%-define(TLOGN(X,Y), io:format("~p~p: " ++ X ++ "~n", [?MODULE, ?LINE] ++ Y)).
-define(TLOGN(X,Y), ok).

%-define(TIMELOG(X,Y), io:format("Transaction time: phase ~p took ~p ms ~n", [X,Y])).
-define(TIMELOG(X,Y), ok).
