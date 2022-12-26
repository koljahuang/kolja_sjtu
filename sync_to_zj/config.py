
from dynaconf import Dynaconf

settings = Dynaconf(
    envvar_prefix="MYAPP",
    settings_files=['configs/.secrets_dev.toml', 
                    'configs/.secrets.toml', 
                    'configs/settings_dev.toml', 
                    'configs/settings.toml'],
    environments=True,
    env='dev',
)

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load these files in the order.
