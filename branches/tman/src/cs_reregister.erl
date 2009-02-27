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
%%% File    : cs_reregister.erl
%%% Author  : Thorsten Schuett <schuett@csr-pc11.zib.de>
%%% Description : reregister with boot nodes
%%%
%%% Created : 11 Oct 2007 by Thorsten Schuett <schuett@csr-pc11.zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(cs_reregister).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([reregister/0]).

reregister() ->
    RegisterMessage = {register, cs_send:this()},
    reregister(config:register_hosts(), RegisterMessage),
    erlang:send_after(config:reregisterInterval(), self(), {reregister}).

reregister(failed, Message)->
    cs_send:send(config:bootPid(), Message);
reregister(Hosts, Message) ->
    lists:foreach(fun (Host) -> 
			  cs_send:send(Host, Message)
		  end, 
		  Hosts).


