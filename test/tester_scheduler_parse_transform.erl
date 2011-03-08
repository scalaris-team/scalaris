%% ``The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved via the world wide web at http://www.erlang.org/.
%% 
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%% 
%% The Initial Developer of the Original Code is Ericsson Utvecklings AB.
%% Portions created by Ericsson are Copyright 1999, Ericsson Utvecklings
%% AB. All Rights Reserved.''
%% 
%%     $Id$
%%
-module(tester_scheduler_parse_transform).

%% A transformer which replaces comm:send* calls by calls to
%% tester_scheduler:send*.

-export([parse_transform/2]).

parse_transform(Forms, _Options) ->
    forms(Forms).

%% forms(Fs) -> lists:map(fun (F) -> form(F) end, Fs).

forms([F0|Fs0]) ->
    F1 = form(F0),
    Fs1 = forms(Fs0),
    [F1|Fs1];
forms([]) -> [].

%% -type form(Form) -> Form.
%%  Here we show every known form and valid internal structure. We do not
%%  that the ordering is correct!

%% First the various attributes.
form({attribute,Line,module,Mod}) ->
    {attribute,Line,module,Mod};
form({attribute,Line,file,{File,Line}}) ->	%This is valid anywhere.
    {attribute,Line,file,{File,Line}};
form({attribute,Line,export,Es0}) ->
    Es1 = erl_id_trans:farity_list(Es0),
    {attribute,Line,export,Es1};
form({attribute,Line,import,{Mod,Is0}}) ->
    Is1 = erl_id_trans:farity_list(Is0),
    {attribute,Line,import,{Mod,Is1}};
form({attribute,Line,compile,C}) ->
    {attribute,Line,compile,C};
form({attribute,Line,record,{Name,Defs0}}) ->
    Defs1 = erl_id_trans:record_defs(Defs0),
    {attribute,Line,record,{Name,Defs1}};
form({attribute,Line,asm,{function,N,A,Code}}) ->
    {attribute,Line,asm,{function,N,A,Code}};
form({attribute,Line,Attr,Val}) ->		%The general attribute.
    {attribute,Line,Attr,Val};
% match the three comm:send* function calls
form({function,Line,_Name0 = send, _Arity0 = 2,Clauses0}) ->
    {Name,Arity,Clauses} = comm_send(Clauses0), %function(Name0, Arity0, Clauses0),
    {function,Line,Name,Arity,Clauses};
form({function,Line,_Name0 = send_local, _Arity0 = 2,_Clauses0}) ->
    {Name,Arity,Clauses} = comm_send_local(), %function(Name0, Arity0, Clauses0),
    {function,Line,Name,Arity,Clauses};
form({function,Line,_Name0 = send_local_after, _Arity0 = 3,_Clauses0}) ->
    {Name,Arity,Clauses} = comm_send_local_after(), %function(Name0, Arity0, Clauses0),
    {function,Line,Name,Arity,Clauses};
form({function,Line,Name0,Arity0,Clauses0}) ->
    {Name,Arity,Clauses} = erl_id_trans:function(Name0, Arity0, Clauses0),
    {function,Line,Name,Arity,Clauses};
%% Mnemosyne, ignore...
form({rule,Line,Name,Arity,Body}) ->
    {rule,Line,Name,Arity,Body}; % Dont dig into this
%% Extra forms from the parser.
form({error,E}) -> {error,E};
form({warning,W}) -> {warning,W};
form({eof,Line}) -> {eof,Line}.

comm_send(Clauses0) ->
    ct:pal("comm_send ~w", [Clauses0]),
    Head = [{var, 100, 'Pid'}, {var, 100, 'Message'}],
    Guard = [],
    Args = [{var, 100, 'Pid'}, {var, 100, 'Message'}],
    %F = {function, tester_scheduler, comm_send, 2},
    F = {remote,102,{atom,102,tester_scheduler},{atom,102,comm_send}},
    Body = [{call, 101, F, Args}],
    Clause = {clause, 100, Head, Guard, Body},
    Clauses = [Clause],
    {send, 2, Clauses}.

comm_send_local() ->
    Head = [{var, 100, 'Pid'}, {var, 100, 'Message'}],
    Guard = [],
    Args = [{var, 100, 'Pid'}, {var, 100, 'Message'}],
    F = {remote, 102, {atom, 102,tester_scheduler}, {atom,102,comm_send_local}},
    Body = [{call, 101, F, Args}],
    Clause = {clause, 100, Head, Guard, Body},
    Clauses = [Clause],
    {send_local, 2, Clauses}.

comm_send_local_after() ->
    Head = [{var, 100, 'Delay'}, {var, 100, 'Pid'}, {var, 100, 'Message'}],
    Guard = [],
    Args = [{var, 100, 'Delay'}, {var, 100, 'Pid'}, {var, 100, 'Message'}],
    F = {remote, 102, {atom, 102, tester_scheduler}, {atom, 102, comm_send_local_after}},
    Body = [{call, 101, F, Args}],
    Clause = {clause, 100, Head, Guard, Body},
    Clauses = [Clause],
    {send_local_after, 3, Clauses}.
