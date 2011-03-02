%% @copyright 2011 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @version $Id$
-module(yaws_SUITE).
-author('schintke@zib.de').
-vsn('$Id$').
-compile(export_all).
-include("unittest.hrl").

all()   -> [yaws_patch_applied].
suite() -> [ {timetrap, {seconds, 1}} ].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

yaws_patch_applied(_Config) ->
    %% By default json:encode/1 rejects lists that are not strings.
    %% The patch in contrib/yaws_json_encode_lists_patch.diff allows
    %% to convert lists to {array, List} automatically. This is needed
    %% to support lists as values (for example for pubsub).
    %% Here we check wether the patch is applied to contrib/yaws/src/json.erl
    ?equals_w_note(catch json:encode(["a","b","c"]),
                   json:encode({array, ["a","b","c"]}),
                   "### New yaws version in contrib? "
                   "Please call '(cd contrib; patch -p0 "
                   "< yaws_json_encode_lists_patch.diff)' ###"),
    ok.

