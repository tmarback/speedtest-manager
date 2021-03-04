import argparse
import os
import signal
import socket
import sys
from pathlib import Path

from pkg_resources import require

from .jobs import JobManager
from .control import ManagerServer

DEFAULT_DATADIR = Path( os.environ["HOME"] ) / '.speedtest-manager'

manager: JobManager
server: ManagerServer

def shutdown( *_ ) -> None:

    server.stop()
    manager.shutdown()

    sys.exit( 0 )

def main() -> None:

    signal.signal( signal.SIGTERM, shutdown ) # Set up shutdown handler

    parser = argparse.ArgumentParser( description = "Main server process for a scheduled Speedtest tester." )
    parser.add_argument( '-d', '--datadir', default = DEFAULT_DATADIR, type = Path, help = "The directory where data is stored." )
    parser.add_argument( '-w', '--workers', default = 5,               type = int,  help = "The number of workers to use for handling client connections." )

    network_args = parser.add_argument_group( 'Network Settings', description = "Arguments that control how clients connect to the manager." )

    family_group = network_args.add_mutually_exclusive_group( required = True )
    family_group.add_argument( '-u', '--unix', action = 'store_const', dest = 'family', const = socket.AF_UNIX,  help = "Uses a UNIX socket for connections." )
    family_group.add_argument( '-4', '--ipv4', action = 'store_const', dest = 'family', const = socket.AF_INET,  help = "Uses an IPV4 socket for connections." )
    family_group.add_argument( '-6', '--ipv6', action = 'store_const', dest = 'family', const = socket.AF_INET6, help = "Uses an IPV6 socket for connections." )

    network_args.add_argument( '-a', '--host', default = None, type = str, help = "The address to listen to." )
    network_args.add_argument( '-p', '--port', default = 8090, type = int, help = "The port to receive connnections on." )

    args = parser.parse_args()

    datadir: Path = args.datadir
    datadir.mkdir( mode = 0o770, parents = True, exist_ok = True )

    family: int = args.family
    host: str = args.host
    port: int = args.port
    if family == socket.AF_UNIX:
        address = str( host if host is not None else datadir / 'server.sock' )
    elif family == socket.AF_INET:
        address = ( host if host is not None else '127.0.0.1', port )
    elif family == socket.AF_INET6:
        address = ( host if host is not None else '::1', port )
    else:
        raise ValueError( "Unsupported address family." )

    workers: int = args.workers

    global manager, server
    manager = JobManager( datadir )
    server = ManagerServer( manager, family = family, addr = address, workers = workers )

    manager.start()
    server.start()

if __name__ == '__main__':
    main()