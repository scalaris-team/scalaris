% Copyright 2007-2010 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%
%    Licensed under the Apache License, Version 2.0 (the "License");
%    you may not use this file except in compliance with the License.
%    You may obtain a copy of the License at
%
%        http://www.apache.org/licenses/LICENSE-2.0
%
%    Unless required by applicable law or agreed to in writing, software
%    distributed under the License is distributed on an "AS IS" BASIS,
%    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%    See the License for the specific language governing permissions and
%    limitations under the License.
%
% $Id$

{application, scalaris,
              [{description, "scalaris"},
               {vsn, "0.2"},
               {mod, {scalaris_app, node}},
               {registered, []},
               {applications, [kernel, stdlib]},
               {env, [
                      {config, "scalaris.cfg"},
                      {local_config, "scalaris.local.cfg"},
                      {log_path, "../log"},
                      {docroot, "../docroot_node"}
                      ]
                     }
               ]}.
