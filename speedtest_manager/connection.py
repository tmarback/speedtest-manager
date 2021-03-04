"""
Provides wrappers for the network layer.
"""

import json
import logging
import socket
from abc import ABC, abstractclassmethod, abstractmethod
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass, asdict
from pathlib import Path
from threading import Event, Thread
from typing import Mapping, Tuple, Type, TypeVar, Union, Callable, Sequence

_LOGGER = logging.getLogger( __name__ )

_ENCODING = 'utf-8'
_SOCK_TYPE = socket.SOCK_STREAM

JSONData = Union[int, float, str, bool, Sequence['JSONData'], Mapping[str,'JSONData'], None]

socket.setdefaulttimeout( 5 ) # Abort connections if they hang for too long

class ConnectionError( Exception ):
    """
    Represents an error in the connection between the server and client.
    """

    def __init__( self, cause: str ):

        super().__init__( cause )

class Connection:
    """
    Wrapper for a network connection.

    Note that the connection represented by this class is purely transactional; that is, an
    individual connection only sends one message in each direction (a request and corresponding
    reply).
    """

    _LOGGER = logging.getLogger( f'{__name__}.Connection' )

    def __init__( self, sock: socket.socket ):
        """
        Creates an instance that wraps the given socket.

        :param sock: The socket to wrap.
        """

        self.sock = sock

    @staticmethod
    def encode( data: JSONData ) -> bytes:
        """
        Encodes the given JSON data into bytes.

        :param data: The data to encode.
        :raises ConnectionError: If encoding failed.
        :return: The encoded data.
        """

        try:
            s = json.dumps( data )
            return s.encode( _ENCODING )
        except UnicodeError as e:
            raise ConnectionError( f"Failed to encode message: '{s}'" ) from e

    @staticmethod
    def decode( data: bytes ) -> JSONData:
        """
        Decodes data (encoded with encode()) back into JSON data.

        :param data: The data to decode.
        :raises ConnectionError: If decoding failed or the decoded data is not valid JSON.
        :return: The original JSON data.
        """

        try:
            s = data.decode( _ENCODING )
            return json.loads( s )
        except UnicodeError as e:
            raise ConnectionError( f"Failed to decode message: '{data}'" ) from e
        except json.JSONDecodeError as e:
            raise ConnectionError( f"Message is not valid JSON: '{s}'" ) from e

    def send( self, data: JSONData ) -> None:
        """
        Sends the given JSON data through this connection.

        Note that, after this method is called, the outbound lane of the underlying
        socket is closed, so no more data may be sent over it (but data may still be
        received).

        :param data: The data to send.
        """

        self.sock.sendall( self.encode( data ) )
        self.sock.shutdown( socket.SHUT_WR ) # Signal end of message

    def receive( self ) -> JSONData:
        """
        Receives JSON data through this connection.

        Note that the message data is considered to be all data until the connected host
        signals that no more data will be sent (i.e. the inbound lane of the connection is
        closed). This method will block until that happens or a timeout occurs.

        :return: The received JSON data.
        """

        _LOGGER.debug( "Starting receive" )
        msg = b''
        while ( chunk := self.sock.recv( 2048 ) ) != b'':
            msg += chunk
        _LOGGER.debug( "Received %s", msg )
        return self.decode( msg )

T = TypeVar( 'T', bound = 'Data' )

class Data( ABC ):
    """
    A piece of data transmitted between the client and the server.
    """

    @abstractmethod
    def to_json( self ) -> JSONData:
        """
        Converts this instance to JSON data.

        :return: The converted data.
        """
        pass

    @abstractclassmethod
    def from_json( cls: Type[T], data: JSONData ) -> T:
        """
        Restores an instance from JSON data.

        :param data: The data to restore from.
        :raises ValueError: if the given data does not form a valid instance.
        :return: The built instance.
        """
        pass

@dataclass( frozen = True )
class Request( Data ):
    """
    A request from a client to the server.
    """

    type: str
    data: JSONData
    
    def to_json( self ) -> JSONData:

        return asdict( self )

    @classmethod
    def from_json( cls, data: JSONData ) -> 'Request':

        try:
            return cls( **data )
        except ( KeyError, TypeError ) as e:
            raise ConnectionError( f"JSON does not represent a valid request: '{data}'" ) from e

@dataclass( frozen = True )
class Response( Data ):
    """
    A response from the server to the client.
    """

    result: bool
    data: JSONData

    def to_json( self ) -> JSONData:

        return asdict( self )

    @classmethod
    def from_json( cls, data: JSONData ) -> 'Response':

        try:
            return cls( **data )
        except ( KeyError, TypeError ) as e:
            raise ConnectionError( f"JSON does not represent a valid response: '{data}'" ) from e

class Server( ABC ):

    @property
    def BACKLOG_SIZE( self ) -> int:
        return 5

    def __init__( self, family: int, addr: Union[Tuple[str, int], str, bytes], workers: int ):

        self._family = family
        self._addr = addr
        self._stop = Event()
        self._worker = Thread( target = self._run, name = 'server-main' )
        self._executor: Executor = ThreadPoolExecutor( max_workers = workers, thread_name_prefix = 'server-handler' )

    Handler = Callable[[JSONData], Tuple[bool, JSONData]]

    @property
    @abstractmethod
    def handlers( self ) -> Mapping[str, Handler]:
        pass

    def handle( self, request_type: str, request_data: JSONData ) -> Tuple[bool, JSONData]:
        
        handler = self.handlers[request_type]
        return handler( request_data )

    def start( self ) -> None:
        
        _LOGGER.info( "Server start requested" )
        self._worker.start()

    def stop( self, wait: bool = True ) -> None:

        _LOGGER.info( "Server stop requested" )
        self._stop.set()
        if wait:
            self._worker.join()
        self._executor.shutdown( wait = wait )
        _LOGGER.info( "Server stopped" )

    def _handle_client( self, client_socket: socket.socket ) -> None:

        _LOGGER.debug( "Handling client" )
        try:
            with client_socket:
                conn = Connection( client_socket )
                # Receive request
                msg = conn.receive()
                _LOGGER.debug( "Received message: '%s'", msg )
                # Handle request
                request = Request.from_json( msg )
                _LOGGER.debug( "Handing request %s", request )
                try:
                    success, response_data = self.handle( request.type, request.data )
                    _LOGGER.debug( "Request handler returned %s", "success" if success else "failure"  )
                    response = Response( result = success, data = response_data )
                except Exception:
                    _LOGGER.exception( "Request handler threw an exception." )
                    response = Response( result = None, data = {} )
                # Send response
                conn.send( response.to_json() )
        except socket.timeout:
            _LOGGER.exception( "Connection to client timed out." )
        except ConnectionError:
            _LOGGER.exception( "A communication error was encountered." )
        except Exception:
            _LOGGER.exception( "Unhandled exception in client handler." )

    def _run( self ):

        with socket.socket( family = self._family, type = _SOCK_TYPE ) as server_socket:

            server_socket.settimeout( 1 ) # Shorter timeout to detect termination sooner
            server_socket.bind( self._addr )
            server_socket.listen( self.BACKLOG_SIZE )

            _LOGGER.debug( "Starting server operation" )
            while not self._stop.is_set(): # Run until stop requested

                try:
                    ( client_socket, address ) = server_socket.accept()
                    _LOGGER.info( "New connection from address %s", address )
                    self._executor.submit( self._handle_client, client_socket )
                except socket.timeout:
                    pass # Timeout only to check if stop requested

        if self._family == socket.AF_UNIX:
            Path( self._addr ).unlink() # Clean up the socket

class ServerError( Exception ):

    def __init__( self ):

        super().__init__( "The server encountered an internal error." )

class Client:

    def __init__( self, family: int, addr: Union[Tuple[str, int], str, bytes] ):

        self._family = family
        self._addr = addr

    def _request( self, request_type: str, request_data: JSONData ) -> Tuple[bool, JSONData]:

        request = Request( type = request_type, data = request_data )
        with socket.socket( family = self._family, type = _SOCK_TYPE ) as sock:

            # Connect to server
            try:
                sock.connect( self._addr )
            except socket.timeout as e:
                raise ConnectionError( "Could not connect to server." ) from e
            connection = Connection( sock )
            # Send request
            connection.send( request.to_json() )
            # Receive response
            response_json = connection.receive()

        # Decode response
        response = Response.from_json( response_json )
        if response.result is None:
            raise ServerError()
        else:
            return response.result, response.data
