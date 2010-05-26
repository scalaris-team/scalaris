%  Copyright 2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : trigger_beh.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : trigger behaviour
%%%
%%% Created :  2 Oct 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(trigger_beh).

-author('hennig@zib.de').
-vsn('$Id$').

% for behaviour
-export([behaviour_info/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Behaviour definition
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

behaviour_info(callbacks) ->
    [
     {init, 4},
     {first, 1},
     {next, 2}
    ];

behaviour_info(_Other) ->
    undefined.
