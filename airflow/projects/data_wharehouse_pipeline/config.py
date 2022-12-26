
from dynaconf import Dynaconf

settings = Dynaconf(
    envvar_prefix="DWPIPE",
    settings_files=['.secrets_dev.toml', 
                    '.secrets.toml', 
                    'settings_dev.toml', 
                    'settings.toml'],
    environments=True,
    env='dev',
)

assert settings.LOG_NAME == "dt_wharehouse_pipe"


# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load these files in the order.

