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
%%% File    : cs_api.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Chord# API
%%%
%%% Created : 16 Apr 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: cs_api.erl 463 2008-05-05 11:14:22Z schuett $
-module(cs_api).

-author('schuett@zib.de').
-vsn('$Id: cs_api.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([number_of_nodes/0, node_list/0,
	 get_key/1, set_key/3]).
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc returns the number of nodes known to the boot server
%%      the current erlang instance has to run a boot server
%% @spec number_of_nodes() -> integer()
number_of_nodes()->
    boot ! {get_list, self()},
    receive
	{get_list_response, Nodes} ->
	    util:lengthX(Nodes)
    end.

%% @doc returns all nodes known to the boot server
%%      the current erlang instance has to run a boot server
%% @spec node_list() -> list(pid())
node_list() ->
    boot ! {get_list, self()},
    receive
	{get_list_response, Nodes} ->
	    Nodes
    end.

    
get_key(Key) ->
    TFun = fun(TransLog) ->
		   {Result, TransLog1} = transstore.transaction:read(Key, TransLog),
		   if
                       Result == fail ->
                           {{fail, notfound}, TransLog1};
                       true ->
                           {value, Value} = Result,
                           {{ok, Value}, TransLog1}
                   end
	   end,
    case do_transaction_locally(TFun, fun (X) -> {success, X} end, fun(_X) -> {failure} end, 4000) of
	{success, Value} ->
	    Value;
	{failure, _Reason} ->
	    failure
    end.

set_key(Key, Value, _VersionNr) ->
    TFun = fun(TransLog) ->
		   {Result, TransLog1} = transstore.transaction:write(Key, Value, TransLog),
		   if
                       Result == ok ->
                           {{ok, done}, TransLog1};
                       true ->
                           {{fail, Result}, TransLog1}
                   end
	   end,
    {success} = do_transaction_locally(TFun, fun (_) -> {success} end, fun(_X)->{failure} end, 4000),
    ok.


% I know there is a cs_node in this instance so I will use it directly
%@private
do_transaction_locally(TransFun, SuccessFun, Failure, Timeout) ->
    {ok, PID} = process_dictionary:find_cs_node(),
    PID ! {do_transaction, TransFun, SuccessFun, Failure, self()},
    receive
	X ->
	    X
    after
	Timeout ->
	   do_transaction_locally(TransFun, SuccessFun, Failure, Timeout)
    end.
