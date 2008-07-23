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
%%% File    : boot_util.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Utils for the YAWS server
%%%
%%% Created :  7 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: boot_util.erl 463 2008-05-05 11:14:22Z schuett $
-module(boot_util).

-author('schuett@zib.de').
-vsn('$Id: boot_util.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([bulk_upload/0, bulk_upload_intern/0]).

bulk_upload() ->
    timer:tc(boot_util, bulk_upload_intern, []).

bulk_upload_intern() ->
    cs_send:send(config:bootPid(), {get_list, cs_send:this()}),
    receive
        {get_list_response, Nodes} ->
            %testNodes(Nodes),
            insert(Nodes, "../data/SQLiteWords.txt")
    end.

insert([Node | _], Filename) ->
    ioutils:for_each_line_in_file(Filename, fun (Line, Accum) -> cs_lookup:unreliable_set_key(Node, Line, io_lib:format("~p", [Accum])), Accum + 1 end, [read], 0).
