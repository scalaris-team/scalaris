%  Copyright 2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : ssl_verifier.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : SSL verifier behavior
%%%
%%% Created : 16 Nov 2009 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id $
-module(ssl_verifier).

-behavior(ssl_verifier).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     % verifies a client certificate
     % returns accept or deny
     {verify, 1}
     ].
