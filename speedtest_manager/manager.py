import os
from pathlib import Path

from .jobs import JobManager

DEFAULT_DATADIR = Path( os.environ["HOME"] ) / '.speedtest-manager'

def main():

    datadir = DEFAULT_DATADIR

    datadir.mkdir( mode = 0o770, parents = True, exist_ok = True )

    job_manager = JobManager( datadir )

if __name__ == '__main__':
    main()