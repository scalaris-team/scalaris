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
%%% File    : tsend.erl
%%% Author  : Monika Moser <moser@zib.de>
%%% Description : Includes functions for sending messages to 
%%%               group of nodes, which are involved in the transaction
%%%               - send to all TPs
%%%               - send to all TMs
%%%               Functions extract information on TPs and TMs
%%%                  and replace the corresponding part in the message
%%% Created : 20 Sep 2007 by Monika Moser <moser@zib.de>
%%%-------------------------------------------------------------------
-module(transstore.tsend).

-author('moser@zib.de').
-vsn('$Id$').


-include("trecords.hrl").
-include("../chordsharp.hrl").

-export([send/2, send_to_rtms_with_lookup/2, send_to_participants_with_lookup/2, send_to_participants/2, send_to_rtms/2, send_to_tp/2, tell_rtms/1, send_to_client/2, send_prepare_item/2, send_vote_to_rtms/2]).

-import(cs_lookup).
-import(cs_send).
-import(dict).
-import(erlang).
-import(io).
-import(lists).
-import(process_dictionary).
-import(?RT).
-import(util).



send_to_rtms_with_lookup(TID, Message)->
    RTMKeys = transaction:getRTMKeys(TID),
    ?TLOG("sent_to_rtms_with_lookup"),
    lists:map(fun(RKey) -> 
		      {MessName, TMMessage} = Message,
		      NewTMMessage = TMMessage#tm_message{tm_key = RKey},
		      cs_lookup:unreliable_lookup(RKey, {MessName, NewTMMessage})
	      end,
	      RTMKeys).
		      
send_to_participants_with_lookup(TMState, Message)->
    ?TLOG("sent_to_participants_with_lookup"),
    Keys = dict:fetch_keys(TMState#tm_state.items),
    lists:map(fun(Key)-> send_to_replica_with_lookup(Key, Message) end, Keys).

send_to_replica_with_lookup(Key, Message)->
    ?TLOG("send_to_replica_with_lookup"),
    ReplKeys = ?RT:get_keys_for_replicas(Key),
    {MessName, MessText} = Message,
    lists:map(fun(RKey) -> 
		      NewMessText = MessText#tp_message{item_key = RKey, orig_key = Key},
		      cs_lookup:unreliable_lookup(RKey, {MessName, NewMessText})
	      end, 
	      ReplKeys).
	
send_to_participants(TMState, Message)->
    dict:map(fun(_Key, Item) ->
		     send_to_tp(Item, Message) end, TMState#tm_state.items).

send_to_rtms(TMState, Message)->
    lists:map(fun({_Key, Address, _})->
		      cs_send:send(Address, Message)
	      end,
	      TMState#tm_state.rtms).

send(Address, Message)->
    cs_send:send(Address, Message).

send_vote_to_rtms(RTMS, Vote)->
    lists:map(fun(RTM)->
		      {_, Address, _} = RTM,
%		      ?TLOGN("message ~p", [{vote, self(), Vote}]),
		      cs_send:send(Address, {vote, cs_send:this(), Vote})
	      end,
	      RTMS).

send_to_tp(Item, Message)->
    {MessName, MessText} = Message,
    lists:map(fun({RKey, TP})->
		      NewMessText = MessText#tp_message{item_key = RKey},
		      cs_send:send(TP, {MessName, NewMessText}) end, Item#tm_item.tps).


%@private
%% get_pid(Id) ->
%%     InstanceId = erlang:get(instance_id),
%%     if
%% 	InstanceId == undefined ->
%% 	    io:format("~p~n", [util:get_stacktrace()]);
%% 	true ->
%% 	    ok
%%     end,
%%     process_dictionary:lookup_process(InstanceId, Id).

tell_rtms(TMState)->
    lists:map(fun({_Key, Address, Ballot})->
		      cs_send:send(Address, {rtms, TMState#tm_state.rtms, Ballot})
	      end,
	      TMState#tm_state.rtms).

send_to_client(Pid, Message)->
    cs_send:send(Pid, {trans, Message}).

send_prepare_item(TMState, Item)->
    TPs = Item#tm_item.tps,
    lists:map(fun({RKey, TP}) -> 
		      NItem = tp_log:new_item(Item#tm_item.key, 
							  RKey, 
							  Item#tm_item.value,
							  Item#tm_item.version, 
							  Item#tm_item.operation, 
							  TMState#tm_state.rtms),
		      Message = {validate, TMState#tm_state.transID, NItem},
		      cs_send:send(TP, Message) 
	      end, 
	      TPs).
