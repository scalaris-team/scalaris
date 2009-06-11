{application, boot_cs,
	[{description, "Chordsharp boot version 0.1"},
	{vsn, "0.1"},
	{mod, {boot_app, []}},
	{registered, [boot]},
	{applications, [kernel, stdlib]},
    {env, [
            {config, "scalaris.cfg"},
            {local_config, "scalaris.local.cfg"},
            {log_path, "../log"},
            {docroot, "../docroot"}
          ]
    }
	]}.
