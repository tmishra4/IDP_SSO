#!"E:\Python Projects\IDP_SSO\venv\Scripts\python.exe"
# EASY-INSTALL-ENTRY-SCRIPT: 'pysnowflake==0.1.3','console_scripts','snowflake_start_server'
__requires__ = 'pysnowflake==0.1.3'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('pysnowflake==0.1.3', 'console_scripts', 'snowflake_start_server')()
    )
