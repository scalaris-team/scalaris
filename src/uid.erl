%  @copyright 2012 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Provides unique process-local and global IDs.
%% @end
%% @version $Id$
-module(uid).
-author('kruber@zib.de').
-vsn('$Id$').

-export_type([global_uid/0]).

-export([get_pids_uid/0, get_global_uid/0,
         is_my_old_uid/1, is_old_uid/2,
         from_same_pid/2]).

-opaque global_uid() :: {pos_integer(), pid()}.

-spec get_pids_uid() -> pos_integer().
get_pids_uid() ->
    Result = case erlang:get(pids_uid_counter) of
                 undefined ->
                     %% Same pid may be reused in the same VM, so we
                     %% get a VM unique offset to start
                     %% It is not completely safe, but safe enough
                     element(1, erlang:statistics(reductions));
                 Any -> Any + 1
             end,
    erlang:put(pids_uid_counter, Result),
    Result.

-spec get_global_uid() -> global_uid().
get_global_uid() ->
    % note: Erlang makes the local pid() globally unique by adding the node
    %       name when transferring it
    _Result = {get_pids_uid(), self()}
    %% , term_to_binary(_Result, [{minor_version, 1}])
    .

%% @doc Checks whether the given GUID is an old incarnation of a GUID from
%%      my node.
-spec is_my_old_uid(pos_integer() | global_uid()) -> boolean() | remote.
is_my_old_uid({LocalUid, Pid}) ->
    case comm:this() of
        Pid -> is_my_old_uid(LocalUid);
        _   -> remote
    end;
is_my_old_uid(Id) when is_integer(Id) ->
    LastUid = case erlang:get(pids_uid_counter) of
                  undefined -> 0;
                  Any -> Any
              end,
    Id =< LastUid;
is_my_old_uid(_Id) ->
    false.

%% @doc Checks whether GUID1 is an old incarnation of GUID2.
-spec is_old_uid(GUID1::global_uid(), GUID2::global_uid()) -> boolean().
is_old_uid({LocalUid1, Pid}, {LocalUid2, Pid}) when LocalUid1 < LocalUid2 ->
    true;
is_old_uid(_GUID1, _GUID2) ->
    false.

%% @doc Checks whether GUID1 is from the same process as GUID2.
-spec from_same_pid(GUID1::global_uid(), GUID2::global_uid()) -> boolean().
from_same_pid({_LocalUid1, Pid}, {_LocalUid2, Pid}) ->
    true;
from_same_pid(_GUID1, _GUID2) ->
    false.
