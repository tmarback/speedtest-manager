"""
Provides an interface for performing speedtests.

Classses:

    TestError

Functions:

    run_test( timeout ) -> Mapping

Constants:

    DEFAULT_TIMEOUT - Default timeout for a test
"""

import json
import subprocess
from typing import Iterable, Mapping, Optional

DEFAULT_TIMEOUT: float = 10 * 60 # Default amount of time (in seconds) to wait for

# Exception that indicates some error occurred during testing
class TestError( RuntimeError ):
    """
    Exception that indicates some error occurred during testing.
    """

    def __init__( self, cause: str, stdout: Optional[str], stderr: Optional[str] ):
        """
        Creates an instance with the given cause.

        :param cause: The cause of the exception.
        """

        super().__init__( cause )

        self._stdout = stdout
        self._stderr = stderr

    @property
    def stdout( self ) -> Optional[str]:
        """
        The standard output of the command.
        """
        return self._stdout

    @property
    def stderr( self ) -> Optional[str]:
        """
        The standard error of the command.
        """
        return self._stderr

def _run( args: Iterable[str], timeout: float ) -> subprocess.CompletedProcess:
    """
    Calls the speedtest command with the given arguments using a subprocess.

    :param args: The arguments to give to the command.
    :param timeout: The number of seconds to wait for the test to complete before aborting it, defaults to DEFAULT_TIMEOUT.
    :raises TestError: if an error occured while executing the command.
    :return: The completed process.
    """

    # Include license acceptance options to be safe
    command = [ 'speedtest', '--accept-license', '--accept-gdpr', *args ]

    try:
        return subprocess.run( command, capture_output = True, timeout = timeout, check = True, text = True )
    except subprocess.TimeoutExpired as e:
        raise TestError( f"Test process took too long (more than {e.timeout} seconds)", stdout = e.stdout, stderr = e.stderr ) from e
    except subprocess.CalledProcessError as e:
        raise TestError( f"Test process failed with code {e.returncode}", stdout = e.stdout, stderr = e.stderr ) from e
    except subprocess.SubprocessError as e:
        raise TestError( f"Error while calling test process", stdout = None, stderr = None ) from e

def get_version() -> str:
    """
    Retrieves the version string for the currently accessible speedtest command.

    :raises TestError: if an error occured while executing the command.
    :return: The version line (the first line of the command with the --version argument).
    """

    output: str = _run( [ '--version' ], timeout = 0.1 ).stdout
    return output.strip().splitlines()[0].strip()

def run_test( server_id: int = None, server_name: str = None, timeout: float = DEFAULT_TIMEOUT ) -> Mapping:
    """
    Performs a network test with speedtest.

    :param server_id: If specified, the ID of the server to test against.
    :param server_name: If specified, the hostname of the server to test against.
    :param timeout: The number of seconds to wait for the test to complete before aborting it, defaults to DEFAULT_TIMEOUT.
    :raises TestError: if an error occured while executing the test.
    :return: The JSON data returned by the test.
    """

    args = []
    if server_name is not None:
        args.append( f'--host={server_name}' )
    if server_id is not None:
        args.append( f'--server-id={server_id}' )

    result = _run( args + [ '--progress=no', '--format=json' ], timeout )

    try:
        result_data = json.loads( result.stdout )
    except json.JSONDecodeError as e:
        raise TestError( f"Test subprocess returned invalid JSON: {result.stdout}", stdout = result.stdout, stderr = result.stderr ) from e

    return result_data