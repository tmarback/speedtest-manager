import argparse
import json
import logging
import os
import re
import socket
from datetime import datetime, timedelta
from pathlib import Path

from dateutil import parser
import pytz

from .control import ManagerClient
from .logging import setup_logging
from .jobs import Job

DEFAULT_DATADIR = Path( os.environ["HOME"] ) / '.speedtest-manager'

LOGLEVELS = frozenset( logging._nameToLevel.keys() )
DEFAULT_LOGLEVEL = logging.getLevelName( logging.NOTSET )
VERBOSE_LOGLEVEL = logging.getLevelName( logging.DEBUG )

INTERVAL_PATTERN = re.compile( r'(\d+)([smhd])' )

def parse_interval( s: str ) -> timedelta:

    m = INTERVAL_PATTERN.match( s )
    if m is None:
        raise ValueError( f"Not a valid interval: {s}" )
    value =int( m.group( 1 ) )
    unit = m.group( 2 )
    if unit == 's':
        return timedelta( seconds = value )
    elif unit == 'm':
        return timedelta( minutes = value )
    elif unit == 'h':
        return timedelta( hours = value )
    elif unit == 'd':
        return timedelta( days = value )
    else:
        raise RuntimeError( "Should never happen" )

def parse_time( s: str ) -> datetime:

    return parser.isoparse( s ).astimezone( pytz.utc )

def main() -> None:

    parser = argparse.ArgumentParser( description = "Main server process for a scheduled Speedtest tester." )
    parser.add_argument( '--version', action = 'version', version = 'Speedtest Manager Client 0.1.0', help = "Display the current version and exit" )
    parser.add_argument( '-d', '--datadir', default = DEFAULT_DATADIR, type = Path, help = "The directory where data is stored, used only to calculate relative paths for Unix sockets" )
    
    logging_args = parser.add_argument_group( 'Logging Settings', description = "Arguments that controls logging output." )
    
    loglevel_group = logging_args.add_mutually_exclusive_group( required = False )
    loglevel_group.add_argument( '-l', '--loglevel', type = str, choices = LOGLEVELS, dest = 'loglevel', 
        help = "The level of logging to use."
    )
    loglevel_group.add_argument( '-v', '--verbose', action = 'store_const', const = VERBOSE_LOGLEVEL, dest = 'loglevel', 
        help = f"Output all logging. This is equivalent to --loglevel {VERBOSE_LOGLEVEL}."
    )

    network_args = parser.add_argument_group( 'Network Settings', description = "Arguments that control how the client connects to the manager." )

    family_group = network_args.add_mutually_exclusive_group( required = False )
    family_group.add_argument( '-u', '--unix', action = 'store_const', dest = 'family', const = socket.AF_UNIX,  help = "Uses a UNIX socket for connections" )
    family_group.add_argument( '-4', '--ipv4', action = 'store_const', dest = 'family', const = socket.AF_INET,  help = "Uses an IPV4 socket for connections" )
    family_group.add_argument( '-6', '--ipv6', action = 'store_const', dest = 'family', const = socket.AF_INET6, help = "Uses an IPV6 socket for connections" )

    network_args.add_argument( '-a', '--host', default = None, type = str, help = "The address to connect to" )
    network_args.add_argument( '-p', '--port', default = 8090, type = int, help = "The port to connect to" )

    parser.set_defaults( family = socket.AF_UNIX, loglevel = DEFAULT_LOGLEVEL )

    subparsers = parser.add_subparsers( required = True, dest = 'operation', title = "Operations", description = "Operations that can be performed in the manager." )

    ##### New job

    new_job_parser = subparsers.add_parser( 'new', help = "Creates a new job", description = "Creates a new job in the system with the given parameters." )

    new_job_parser.add_argument( 'id', type = str, help = "The ID of the job" )

    new_job_parser.add_argument( '-t', '--title', type = str, help = "The title of the job (purely for readability purposes)" )
    new_job_parser.add_argument( '-i', '--interval', type = parse_interval, default = None,
        help = "The interval between job executions. If not specified, the job is only ran once, and the end time is ignored."
    )
    new_job_parser.add_argument( '-s', '--start', type = parse_time, default = None, help = "When to start the job. If not specified, starts immediately." )
    new_job_parser.add_argument( '-e', '--end',   type = parse_time, default = None, help = "When to stop the job. If not specified, the job will run until manually stopped." )

    server_id_group = new_job_parser.add_mutually_exclusive_group( required = True )
    server_id_group.add_argument( '--server-id',   type = int, default = None, help = "The ID of the server to use for the job" )
    server_id_group.add_argument( '--server-name', type = str, default = None, help = "The hostname of the server to use for the job" )

    def new_job( client: ManagerClient, args ) -> None:

        job = Job(
            id = args.id,
            title = args.title,
            server_id = args.server_id,
            server_name = args.server_name,
            interval = args.interval,
            start = args.start,
            end = args.end,
        )
        id = client.new_job( job )
        
        print( f"Created job with ID '{id}'." )

    new_job_parser.set_defaults( func = new_job )

    ##### Get results

    get_results_parser = subparsers.add_parser( 'results', help = "Retrieves job results", description = "Retrieves the results obtained so far by a registered job." )

    get_results_parser.add_argument( 'id', type = str, help = 'The ID of the job' )

    def get_results( client: ManagerClient, args ) -> None:

        results = client.get_results( args.id )
        print( json.dumps( results ) )

    get_results_parser.set_defaults( func = get_results )

    ##### Run program

    args = parser.parse_args()

    datadir: Path = args.datadir

    loglevel: int = getattr( logging, args.loglevel )
    setup_logging( True, logdir = None, level = loglevel )

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

    client = ManagerClient( family, address )
    args.func( client, args )

if __name__ == '__main__':
    main()