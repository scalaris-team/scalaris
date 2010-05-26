%  Copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : pubsub_api.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Publish API function
%%%
%%% Created : 17 Sep 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id $
-module(pubsub_api).

-author('schuett@zib.de').
-vsn('$Id$').

-export([publish/2, subscribe/2, unsubscribe/2, get_subscribers/1]).

%%====================================================================
%% public functions
%%====================================================================

%% @doc publishs an event under a given topic.
%%      called e.g. from the java-interface
%% @spec publish(string(), string()) -> ok
publish(Topic, Content) ->
    Subscribers = get_subscribers(Topic),
    lists:foreach(fun (Subscriber) -> 
			  pubsub_publish:publish(Subscriber, Topic, Content) 
		  end, 
		  Subscribers),
    ok.

%% @doc subscribes a url for a topic.
%%      called e.g. from the java-interface
%% @spec subscribe(string(), string()) -> ok | {fail, term()}
subscribe(Topic, URL) ->
    TFun = fun(TransLog) ->
		   {{Success, _ValueOrReason} = Result, TransLog1} = transaction_api:read(Topic, TransLog),
		   {Result2, TransLog2} = 
                       case Success of
                           fail ->
                               transaction_api:write(Topic, [URL], TransLog); %obacht: muss TransLog sein!
                           _Any ->
                               {value, Subscribers} =
                                   case Result of
                                       {value, empty_val} -> {value, []};
                                       _ -> Result
                                   end,
                               case [ X || X <- Subscribers, X =:= URL ] of
                                   [] ->
                                       transaction_api:write(Topic, [URL | Subscribers], TransLog1);
                                   _ -> {ok, TransLog1}
                               end
                       end,
                   if
		       Result2 =:= ok ->
			   {{ok, ok}, TransLog2};
		       true ->
			   {Result2, TransLog2}
		   end
	   end,
    transaction_api:do_transaction(TFun, fun (_) -> ok end, fun (X) -> {fail, X} end).

%% @doc unsubscribes a url for a topic.
-spec(unsubscribe/2 :: (string(), string()) -> ok | {fail, any()}).
unsubscribe(Topic, URL) ->
    TFun = fun(TransLog) ->
		   {Subscribers, TransLog1} = transaction_api:read2(TransLog, Topic),
		   case (Subscribers =/= empty_val) andalso
                       lists:member(URL, Subscribers) of
                       true ->
			   NewSubscribers = lists:delete(URL, Subscribers),
			   TransLog2 = transaction_api:write2(TransLog1, Topic, NewSubscribers),
			   {{ok, ok}, TransLog2};
		       false ->
			   {{fail, not_found}, TransLog}
		   end
	   end,
    transaction_api:do_transaction(TFun, fun (_) -> ok end, fun (X) -> {fail, X} end).

%% @doc queries the subscribers of a query
%% @spec get_subscribers(string()) -> [string()]
get_subscribers(Topic) ->
    {Res, _Value} = transaction_api:quorum_read(Topic),
    if
        %% Fl is either empty/fail or the Value/Subscribers
	Res =:= empty_val ->
	    [];
        Res =:= fail ->
            [];
	true ->
	    Res
    end.
