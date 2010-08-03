% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc operations on the transaction log of a transaction
%%      participant functions to extract certain information
%% @end
%% @version $Id$
-module(tp_log).
-author('moser@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("trecords.hrl").

-export([get_log/1, new_item/6, new_logentry/3, add_to_undecided/3, remove_from_undecided/4]).

%%% Structure of the transaction log:
%%% top gb_tree:  key: TransID
%%%               value: Transaction data (transaction record)
%%%

-spec new_item(Key::?RT:key(), RKey::any(), Value::?DB:value(), Version::?DB:version(), Operation::read | write, TransactionManagers::[{SelfKey::?RT:key(), comm:mypid(), LeaderBallot::non_neg_integer()}]) -> #item{}.
new_item(Key, RKey, Value, Version, Operation, TransactionManagers) ->
    #item{
	  key = Key,
	  rkey = RKey,
	  value = Value,
	  version = Version,
	  operation = Operation,
	  tms = TransactionManagers
	 }.

-spec new_logentry(Status::prepared | local_abort, TransID::any(), Item::#item{}) -> #logentry{}.
new_logentry(Status, TransID, Item)->
    #logentry{
	      status = Status,
	      key = Item#item.key,
	      rkey = Item#item.rkey,
	      value = Item#item.value,
	      version = Item#item.version,
	      operation = Item#item.operation,
	      transactionID = TransID,
	      tms = Item#item.tms
	     }.

-spec get_log(State::dht_node_state:state()) -> #translog{}.
get_log(State) ->
    dht_node_state:get(State, trans_log).

-spec add_to_undecided(State::dht_node_state:state(), TransID::any(), LogEntry::#logentry{}) -> dht_node_state:state().
add_to_undecided(State, TransID, LogEntry) ->
    TransLog = dht_node_state:get(State, trans_log),
    NewTransInLogList =
        case gb_trees:lookup(TransID, TransLog#translog.undecided) of
            {value, TransInLogList} -> lists:append([LogEntry], TransInLogList);
            none                    -> [LogEntry]
        end,
    NewTransInLog = gb_trees:enter(TransID, NewTransInLogList, TransLog#translog.undecided),
    dht_node_state:set_trans_log(State, TransLog#translog{undecided=NewTransInLog}).

-spec remove_from_undecided(State::dht_node_state:state(), TransID::any(), TransLog::#translog{}, TransLogUndecided::gb_tree()) -> dht_node_state:state().
remove_from_undecided(State, TransID, TransLog, TransLogUndecided) ->
    NewLogEntries = gb_trees:delete(TransID, TransLogUndecided),
    dht_node_state:set_trans_log(State, TransLog#translog{undecided=NewLogEntries}).
