import argparse
import logging
import os
import signal
import socket
from pathlib import Path
from threading import Event

from . import __version__
from .jobs import JobManager
from .control import ManagerServer
from .logging import setup_logging

DEFAULT_DATADIR = Path( os.environ["HOME"] ) / '.speedtest-manager'
DEFAULT_LOGDIR = Path( 'log' )

LOGLEVELS = frozenset( logging._nameToLevel.keys() )
DEFAULT_LOGLEVEL = logging.getLevelName( logging.INFO )
VERBOSE_LOGLEVEL = logging.getLevelName( logging.DEBUG )
QUIET_LOGLEVEL   = logging.getLevelName( logging.WARNING )

manager: JobManager
server: ManagerServer

quit_event: Event = Event()

def shutdown( *_ ) -> None:

    logging.info( "Shutdown request received." )

    server.stop()
    manager.shutdown()

    logging.info( "Shutdown completed." )

    quit_event.set()

def main() -> None:

    signal.signal( signal.SIGTERM, shutdown ) # Set up shutdown handler
    signal.signal( signal.SIGINT,  shutdown )

    parser = argparse.ArgumentParser( description = "Main server process for a scheduled Speedtest tester." )
    parser.add_argument( '--version', action = 'version', version = f'Speedtest Manager Server {__version__}', help = "Display the current version and exit" )
    parser.add_argument( '-d', '--datadir', default = DEFAULT_DATADIR, type = Path, help = "The directory where data is stored" )
    parser.add_argument( '-w', '--workers', default = 5,               type = int,  help = "The number of workers to use for handling client connections" )
    
    logging_args = parser.add_argument_group( 'Logging Settings', description = "Arguments that controls logging output." )
    
    loglevel_group = logging_args.add_mutually_exclusive_group( required = False )
    loglevel_group.add_argument( '-l', '--loglevel', type = str, choices = LOGLEVELS, dest = 'loglevel', 
        help = "The level of logging to use."
    )
    loglevel_group.add_argument( '-v', '--verbose', action = 'store_const', const = VERBOSE_LOGLEVEL, dest = 'loglevel', 
        help = f"Output all logging. This is equivalent to --loglevel {VERBOSE_LOGLEVEL}."
    )
    loglevel_group.add_argument( '-q', '--quiet', action = 'store_const', const = QUIET_LOGLEVEL, dest = 'loglevel', 
        help = f"Minimize logging, outputting only important messages. This is equivalent to --loglevel {QUIET_LOGLEVEL}." 
    )

    logging_args.add_argument( '--no-console', '--no-log-console', action = 'store_false', dest = 'log_console', 
        help = "If specified, logging output will not be sent to the console."
    )
    logging_args.add_argument( '--no-file',    '--no-log-file',    action = 'store_false', dest = 'log_file',
        help = "If speficied, logging output will not be saved to a file."
    )
    logging_args.add_argument( '--logdir', default = DEFAULT_LOGDIR, type = Path, 
        help = ( "The directory where log files should be stored. If the path is not absolute, it is relative to the "
                 "data directory specified by the --datadir option." )
    )

    network_args = parser.add_argument_group( 'Network Settings', description = "Arguments that control how clients connect to the manager." )

    family_group = network_args.add_mutually_exclusive_group( required = False )
    family_group.add_argument( '-u', '--unix', action = 'store_const', dest = 'family', const = socket.AF_UNIX,  help = "Uses a UNIX socket for connections" )
    family_group.add_argument( '-4', '--ipv4', action = 'store_const', dest = 'family', const = socket.AF_INET,  help = "Uses an IPV4 socket for connections" )
    family_group.add_argument( '-6', '--ipv6', action = 'store_const', dest = 'family', const = socket.AF_INET6, help = "Uses an IPV6 socket for connections" )

    network_args.add_argument( '-a', '--host', default = None, type = str, help = "The address to listen to" )
    network_args.add_argument( '-p', '--port', default = 8090, type = int, help = "The port to receive connnections on" )

    parser.set_defaults( family = socket.AF_UNIX, loglevel = DEFAULT_LOGLEVEL )

    args = parser.parse_args()

    datadir: Path = args.datadir
    datadir.mkdir( mode = 0o770, parents = True, exist_ok = True )

    loglevel: int = getattr( logging, args.loglevel )
    setup_logging( console_output = args.log_console, logdir = datadir / args.logdir if args.log_file else None, level = loglevel )

    logging.info( "Program starting." )

    family: int = args.family
    host: str = args.host
    port: int = args.port
    if family == socket.AF_UNIX:
        address = str( datadir / ( host if host is not None else 'server.sock' ) )
    elif family == socket.AF_INET:
        address = ( host if host is not None else '127.0.0.1', port )
    elif family == socket.AF_INET6:
        address = ( host if host is not None else '::1', port )
    else:
        raise ValueError( "Unsupported address family." )

    workers: int = args.workers

    global manager, server
    manager = JobManager.initialize( datadir )
    server = ManagerServer( manager, family = family, addr = address, workers = workers )

    manager.start()
    server.start()

    quit_event.wait() # Wait until its time to quit

if __name__ == '__main__':
    main()