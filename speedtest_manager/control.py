import logging
from typing import Optional, Union, Tuple, Mapping, Set, Sequence

from .connection import Server, Client, JSONData
from .jobs import Job, JobManager, JobError

_LOGGER = logging.getLogger( __name__ )

class SpeedtestError( RuntimeError ):
    pass

class ManagerServer( Server ):

    def __init__( self, manager: JobManager, family: int, addr: Union[Tuple[str, int], str, bytes], workers: int ):

        super().__init__( family, addr, workers )
        self.manager = manager

    @staticmethod
    def make_error( cause: str ) -> Tuple[bool, JSONData]:

        return False, { 'cause': cause }

    def handle_new( self, request_data: JSONData ) -> Tuple[bool, JSONData]:

        try:
            job = Job.from_json( request_data )
            self.manager.new_job( job )
        except ValueError:
            _LOGGER.exception( "Received invalid job JSON '%s'", str( request_data ) )
            return self.make_error( "The received data is not a valid job." )
        except JobError as e:
            return self.make_error( str( e ) )

        return True, job.id

    def handle_stop( self, request_data: JSONData ) -> Tuple[bool, JSONData]:
        
        if not isinstance( request_data, str ):
            return self.make_error( "ID must be a string." )
        id: str = request_data

        try:
            job = self.manager.stop_job( id )
        except KeyError:
            return self.make_error( "There is no currently running job with the given ID." )

        return True, job.to_json()

    def handle_delete( self, request_data: JSONData ) -> Tuple[bool, JSONData]:
        
        if not isinstance( request_data, str ):
            return self.make_error( "ID must be a string." )
        id: str = request_data

        try:
            job = self.manager.delete_job( id )
        except KeyError:
            return self.make_error( "There is no job with the given ID." )

        return True, job.to_json()

    def handle_get_job( self, request_data: JSONData ) -> Tuple[bool, JSONData]:
        
        if request_data is None or isinstance( request_data, bool ):
            running: Optional[bool] = request_data
            return True, [ job.to_json() for job in self.manager.get_jobs( running ) ]
        elif isinstance( request_data, str ):
            id: str = request_data
            job = self.manager.get_job( id )
            if job is None:
                return self.make_error( "There is no job with the given ID." )
            else:
                return True, job.to_json()
        else:
            return self.make_error( "Request data may only be a string ID, a boolean, or null." )

    def handle_get_results( self, request_data: JSONData ) -> Tuple[bool, JSONData]:

        if not isinstance( request_data, str ):
            return self.make_error( "ID must be a string." )
        id: str = request_data

        try:
            results = self.manager.get_results( id )
        except KeyError:
            return self.make_error( "There is no job with the given id." )

        return True, results

    @property
    def handlers( self ) -> Mapping[str, Server.Handler]:

        return {
            'new': self.handle_new,
            'stop': self.handle_stop,
            'delete': self.handle_delete,
            'get_job': self.handle_get_job,
            'get_results': self.handle_get_results,
        }

class ManagerClient( Client ):

    def _make_request( self, request_type: str, request_data: JSONData ) -> JSONData:

        success, response = self._request( request_type = request_type, request_data = request_data )
        if success:
            return response
        else:
            raise SpeedtestError( response['cause'] )

    def new_job( self, job: Job ) -> str:
        
        _LOGGER.debug( "Creating job %s", job.to_json() )
        return self._make_request( 'new', job.to_json() )

    def stop_job( self, id: str ) -> Job:

        return Job.from_json( self._make_request( 'stop', id ) )

    def delete_job( self, id: str ) -> Job:

        return Job.from_json( self._make_request( 'delete', id ) )

    def get_job( self, id: str ) -> Job:

        return Job.from_json( self._make_request( 'get_job', id ) )

    def get_jobs( self, running: Optional[bool] ) -> Set[Job]:

        return frozenset( Job.from_json( job ) for job in self._make_request( 'get_job', running ) )
 
    def get_results( self, id: Union[str, Sequence[str]] ) -> Union[JSONData, Mapping[str, JSONData]]:

        if isinstance( id, str ):
            return self._make_request( 'get_results', id )
        else:
            results = {}
            for _id in id: # TODO: Turn this into a single call server-side
                results[_id] = self._make_request( 'get_results', _id )
            return results
