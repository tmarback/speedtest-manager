import json
import logging
import os
import socket
from concurrent import futures
from typing import Mapping, Sequence, Tuple, Union

import pytest

from speedtest_manager.connection import Connection, Server, Client, Data, Request, Response

class TestConnection:

    INPUTS: Mapping = [
        {
            'key1': 1,
            '2': 'hi',
            'map': {
                '1': [ 1, 2, 3 ]
            }
        },
        {
            'method': 'do_thing',
            'params': {
                'id': 45,
                'time': '2020-02-20',
                'path': '/path/to/there'
            }
        },
        {
            'key1': 'value1',
            'key2': 'value2',
            'key3': 'value3',
            'key4': 'value4',
            'key5': 'value5',
        }
    ]

    @pytest.mark.parametrize( 'data', INPUTS )
    def test_encode_decode( self, data ):

        encoded = Connection.encode( data )
        decoded = Connection.decode( encoded )

        assert decoded == data

    @pytest.fixture
    def server_address( self ):

        return ( 'localhost', 8050 )

    @pytest.fixture
    def server_result( self, server_address ):

        def server_runner():

            with socket.create_server( server_address, family = socket.AF_INET ) as ss:
                ss.settimeout( 1 )
                cs, _ = ss.accept()

                return Connection( cs ).receive()

        executor = futures.ThreadPoolExecutor( max_workers = 1 )
        yield executor.submit( server_runner )
        executor.shutdown()

    @pytest.fixture
    def client( self, server_address, server_result ):

        sock = socket.create_connection( server_address, timeout = 1 )
        yield Connection( sock )
        sock.close()

    @pytest.mark.parametrize( 'data', INPUTS )
    def test_send_receive( self, data, client: Connection, server_result: futures.Future ):

        client.send( data )

        assert server_result.result( timeout = 1 ) == data

class TestData:

    INPUTS = [
        Request( type = 'a', data = { 'p': 'b', 'and': 'j' } ),
        Response( result = 'nice', data = { 'i': 1, 'like': { 'big': 23, 'pie': None, 'ya know': {} } } ),
        Response( result = None, data = {} )
    ]

    @pytest.mark.parametrize( 'data', INPUTS )
    def test_json( self, data: Data ):

        json_data = data.to_json()
        assert data.from_json( json_data ) == data

    @pytest.mark.parametrize( 'data', INPUTS )
    def test_json_str( self, data: Data ):

        json_data = data.to_json()
        json_str = json.dumps( json_data )
        assert data.from_json( json.loads( json_str ) ) == data

class TestClientServer:

    class ServerUnderTest( Server ):

        def handle( self, request_type: str, request_data: Mapping ) -> Tuple[bool, Mapping]:
            
            if request_type == 'add':
                if 'op1' not in request_data or 'op2' not in request_data:
                    return False, { 'cause': "Missing operand" }
                op1 = request_data['op1']
                op2 = request_data['op2']
                if not ( isinstance( op1, int ) and isinstance( op2, int ) ):
                    return False, { 'cause': "Operand is not an int" }
                return True, { 'result': op1 + op2 }

            if request_type == 'concatenate':
                if 'elems' not in request_data:
                    return False, { 'cause': "Missing elements to concatenate" }
                return True, { 'concatenated': ''.join( str( e ) for e in request_data['elems'] ) }

            return False, { 'cause': "Unrecognized type" }

    class ClientUnderTest( Client ):

        def add( self, op1: int, op2: int ) -> Tuple[bool, Union[int, str]]:

            result, data = self._request( 'add', {
                'op1': op1,
                'op2': op2
            } )
            return result, data['result'] if result else data['cause']

        def concatenate( self, elems: Sequence ) -> Tuple[bool, str]:

            result, data = self._request( 'concatenate', {
                'elems': elems
            } )
            return result, data['concatenated'] if result else data['cause']

        def broken( self ) -> Tuple[bool, str]:

            result, data = self._request( 'invalid', { 'a': 'b' } )
            return result, data['cause']

    @pytest.fixture( scope = 'class', params = [ 
        ( socket.AF_INET, ( '127.0.0.1', 13460 ) ),
        ( socket.AF_UNIX, '/tmp/test-socket-path' ) 
        ] )
    def net_config( self, request ):

        return request.param

    @pytest.fixture( scope = 'class' )
    def family( self, net_config ):

        return net_config[0]

    @pytest.fixture( scope = 'class' )
    def address( self, net_config ):

        return net_config[1]

    @pytest.fixture( autouse = True, scope = 'class' )
    def server( self, family, address ):

        server = self.ServerUnderTest( family = family, addr = address, workers = 1 )
        server.start()
        yield server
        server.stop()
        
        if family == socket.AF_UNIX:
            os.remove( address ) # Need to clean up socket file manually

    @pytest.fixture( autouse = True )
    def serverlog( self, caplog ):

        caplog.set_level( logging.DEBUG, logger = 'speedtest_manager.connection' )

    @pytest.fixture
    def client( self, family, address ):

        return self.ClientUnderTest( family = family, addr = address )

    @pytest.mark.parametrize( 'op1,op2,exp_result,exp_data', [
        ( 1, 2, True, 3 ),
        ( 5, 2, True, 7 ),
        ( 14, 15, True, 29 ),
        ( 'a', 1, False, "Operand is not an int" ),
        ( 5, 'b', False, "Operand is not an int" ),
    ] )
    def test_add( self, client: ClientUnderTest, op1, op2, exp_result, exp_data ):

        result, data = client.add( op1, op2 )
        assert result == exp_result
        assert data == exp_data

    @pytest.mark.parametrize( 'elems,exp_result,exp_data', [
        ( [ 'a', ' ', 'big', ' ', 'bird' ], True, 'a big bird' ),
        ( [ 'a', 1, 'b' ], True, 'a1b' )
    ] )
    def test_concatenate( self, client: ClientUnderTest, elems: Sequence, exp_result, exp_data ):

        result, data = client.concatenate( elems )
        assert result == exp_result
        assert data == exp_data

    def test_broken( self, client: ClientUnderTest ):

        result, data = client.broken()
        assert not result
        assert data == "Unrecognized type"
