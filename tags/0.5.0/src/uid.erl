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
-vsn('$Id$ ').

-ifdef(with_export_type_support).
-export_type([global_uid/0]).
-endif.

-export([get_pids_uid/0, get_global_uid/0, is_my_old_uid/1]).

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
