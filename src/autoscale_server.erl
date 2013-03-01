% @copyright 2013 Zuse Institute Berlin

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

%% @author Ufuk Celebi <celebi@zib.de>
%% @doc Server for collecting autoscale plot data from autoscale leader(s).
%%
%%      {autoscale_server, true} in the config will enable this service. The
%%      autoscale processes from which data is collected assume that this
%%      server runs in the same VM as mgmt_server (needs to be set in config).
%%
%%      {autoscale_server_plot_path, PATH} needs to be set in order to write
%%      the collected data to the file system (see on({write_to_file})). 
%% @end
%% @version $Id$
-module(autoscale_server).
-author('celebi@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-export([start_link/1, init/1, on/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-type(state() ::
    {PlotData::dict()} |
    unknown_event).

-type(message() ::
    {collect, PlotKey::atom(), Timestamp::pos_integer(), Value::number()} |
    {reset} |
    {write_to_file}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec start_link(ServiceGroup::pid_groups:groupname()) -> {ok, pid()}.
start_link(ServiceGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, null,
                             [{pid_groups_join_as, ServiceGroup, autoscale_server}]).

-spec init(null) -> state().
init(_Options) ->
    {_PlotData = dict:new()}.

-spec on(message(), state()) -> state().
on({collect, PlotKey, Timestamp, Value}, {PlotData}) ->
    {_NewPlotData = dict:append(PlotKey, {Timestamp, Value}, PlotData)};
on({reset}, {_PlotData}) ->
    {_NewPlotData = dict:new()};
on({write_to_file}, {PlotData}) ->
    case PlotPath = config:read(autoscale_server_plot_path) of
        failed -> false;
        _      ->
            lists:foreach(
                fun({PlotKey, Data}) ->
                    Filename = filename:join(PlotPath, PlotKey) ++ ".dat",
                    {ok, File} = file:open(Filename, [write]),
                    lists:foreach(
                        fun({Timestamp, Value}) ->
                            io:format(File, "~p ~p~n", [Timestamp, Value])
                        end, Data),          
                    file:close(File)
                end, dict:to_list(PlotData))
    end,
    {PlotData};
on(_, _) ->
    unknown_event.