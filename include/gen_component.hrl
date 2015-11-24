%  @copyright 2015 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc change the first function in processes from gen_component:start/5
%%      to ?MODULE:start_gen_component/5
%% @end

-export([start_gen_component/5]).
-spec start_gen_component(module(), gen_component:handler(), term(),
                          [gen_component:option()], pid()) ->
                                 no_return() | ok.
start_gen_component(Module, Handler, Args, Options, Self) ->
    gen_component:start(Module, Handler, Args, Options, Self).

