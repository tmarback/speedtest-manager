import functools
import json
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Set, Sequence

from apscheduler.jobstores.base import JobLookupError, ConflictingIdError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Interval, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, Session as SessionClass
from sqlalchemy.sql.expression import null

from . import speedtest
from .connection import Data, JSONData

DATETIME_FORMAT: str = "%Y-%m-%dT%H:%M:%S%z"

@dataclass( frozen = True )
class Job( Data ):
    """
    Represents a job submitted to the system.
    """

    id: str
    title: Optional[str]
    server_id: Optional[int]
    server_name: Optional[str]
    interval: Optional[timedelta]
    start: Optional[datetime]
    end: Optional[datetime]
    running: Optional[bool] = None

    def __post_init__( self ):

        if self.id is None:
            raise ValueError( "The job ID must be specified." )
        if self.server_id is None and self.server_name is None:
            raise ValueError( "Either the server ID or hostname must be specified." )
        if self.interval.total_seconds() < 1:
            raise ValueError( "The interval must be either at least one second or None." )

    def to_json( self ) -> JSONData:

        return asdict( self )

    @classmethod
    def from_json( cls, data: JSONData ) -> 'Job':

        try:
            return cls( **data )
        except ( KeyError, TypeError ) as e:
            raise ValueError( f"JSON does not represent a valid job: '{data}'" ) from e

Base = declarative_base()
class JobMetadata( Base ):
    """
    Internal representation of job metadata
    """

    __tablename__ = 'speedtest_jobs'

    id = Column( String, primary_key = True )
    title = Column( String )
    server_id = Column( Integer )
    server_name = Column( String )
    interval = Column( Interval )
    start = Column( DateTime )
    end = Column( DateTime )
    running = Column( Boolean, nullable = False, default = True )

    def __init__( self, job: Job ):
        """
        Initializes the metadata for the given job.

        :param job: The job to initialize metadata for.
        """

        super().__init__(
            id = job.id,
            title = job.title,
            server_id = job.server_id,
            server_name = job.server_name,
            interval = job.interval,
            start = job.start,
            end = job.end
        )

    def export( self ) -> Job:
        """
        Exports the metadata into an externally shareable format.

        :return: The job this metadata represents.
        """

        return Job(
            id = self.id,
            title = self.title,
            server_id = self.server_id,
            server_name = self.server_name,
            interval = self.interval,
            start = self.start,
            end = self.end,
            running = self.running
        )

class IDExistsError( RuntimeError ):
    """
    Exception that indicates an attempt to add a new job with an existing ID.
    """

    def __init__( self, id: str ):
        """
        Creates a new instance to represent a conflict with the given ID.

        :param id: The ID that caused the conflict.
        """

        super().__init__( f"There is already a job with the ID '{id}'." )
        self._id = id

    @property
    def id( self ) -> str:
        """
        The ID that caused the conflict.
        """

        return self._id

class JobManager:
    """
    Central overseer that manages the measurement jobs.
    """

    def __init__( self, datadir: Path ):
        """
        Initializes a new manager that uses the specified directory to store data.

        :param datadir: The path of the directory where data should be stored.
        """

        database_path = datadir / 'jobs.db'

        self.storage = datadir / 'results'
        self.storage.mkdir( mode = 0o770, exist_ok = True )

        self.engine = create_engine( f'sqlite:///{database_path}' )
        self.Session = sessionmaker( bind = self.engine )

        jobstores = {
            'default': SQLAlchemyJobStore( engine = self.engine )
        }
        executors = {
            'default': ThreadPoolExecutor( 1 )
        }
        job_defaults = {
            'coalesce': True,
            'max_instances': 1
        }
        self.scheduler = BackgroundScheduler( jobstores = jobstores, executors = executors, job_defaults = job_defaults )

    def start( self ):
        """
        Starts processing jobs.
        """

        self.scheduler.start()

    def shutdown( self, wait: bool = True ):
        """
        Shuts down the manager, stopping job processing.

        :param wait: If True, waits for all currently executing jobs to finish before returning.
        """

        self.scheduler.shutdown( wait = wait )

    def output_file( self, job: Job ) -> Path:
        """
        Determines the path to the output file of the job identified by the given ID.

        :param job: The job to get the path for.
        :return: The path of the output file for the given job.
        """

        return self.storage / f'{job.id}.result' # Not really proper JSON

    def run_job( self, job: Job ) -> None:
        """
        Executes the specified job once.

        :param job: The job to execute.
        """

        timestamp = datetime.now()
        try:
            output = speedtest.run_test( server_id = job.server_id, server_name = job.server_name )
            result = {
                'success': True,
                'time': timestamp,
                'result': output
            }
        except speedtest.TestError as e:
            result = {
                'success': False,
                'timestamp': timestamp,
                'error': str( e )
            }

        with open( self.output_file( job ), 'a' ) as f:
            f.write( json.dumps( result ) )
            f.write( ',\n' ) # Line break to make it slightly more readable

    def load_results( self, job: Job ) -> Sequence[JSONData]:
        """
        Loads the results obtained so far for the given job.

        :param job: The job to load results for.
        :return: The results of the given job, as a list of JSON objects.
        """

        with open( self.output_file( job ), 'r' ) as f:
            results = f.read()
        results = '[' + results[:-2] + ']' # Remove trailing comma and line break and add brackets

        return json.loads( results )

    @contextmanager
    def transaction( self ) -> SessionClass:
        """
        Provide a transactional scope around a series of operations.
        """

        session: SessionClass = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def new_job( self, job: Job ) -> None:
        """
        Registers the given job.

        :param job: The job to register.
        :raises IDExistsError: if the ID of the given job is already in use.
        """

        with self.transaction() as session:
            try:
                new_job = JobMetadata( job )
                session.add( new_job )

                if job.interval:
                    trigger = IntervalTrigger( 
                        seconds = int( job.interval.total_seconds() ),
                        start_date = job.start,
                        end_date = job.end
                    )
                else:
                    trigger = DateTrigger(
                        run_date = job.start
                    )

                self.scheduler.add_job( 
                    fun = functools.partial( self, job ),
                    trigger = trigger,
                    id = job.id,
                    name = job.title,
                )
            except ( IntegrityError, ConflictingIdError ) as e:
                raise IDExistsError( job.id ) from e

    def get_job( self, id: str ) -> Optional[Job]:
        """
        Retrieves the job with the given ID.

        :param id: The job ID.
        :return: The job with the given ID, or None if no jobs have the given ID.
        """

        with self.transaction() as session:
            job: JobMetadata = session.query( JobMetadata ).filter_by( id = id ).first()
            return job.export() if job else None

    def get_jobs( self, running: Optional[bool] = None ) -> Set[Job]:
        """
        Retrieves the jobs submitted to the manager.

        :param running: If true, only retrieves currently running jobs. If false, only
                        retrieves completed or stopped jobs. If None, retrieves all jobs.
        :return: The jobs registered in this manager.
        """

        with self.transaction() as session:
            jobs = session.query( JobMetadata )
            if running is not None:
                jobs = jobs.filter_by( running = running )
            return frozenset( job.export() for job in jobs )
    
    def stop_job( self, id: str ) -> Job:
        """
        Stops the jobs specified by the given ID. The job will still be registered in the manager and its
        output will still be retrievable, i.e. the job is finished early.

        :param id: The ID of the shop to be stopped.
        :raises KeyError: if there is no job with the given key.
        :return: The stopped job.
        """

        try:
            self.scheduler.remove_job( id )
        except JobLookupError:
            raise KeyError( f"There are no jobs with the id '{id}'" )

        with self.transaction() as session:
            job: JobMetadata = session.query( JobMetadata ).filter_by( id = id ).first()
            job.running = False
            return job.export()

    def delete_job( self, id: str ) -> Job:
        """
        Removes the job with the given ID from the manager's database. This will stop the job if is
        currently executing, and the job's output is also removed.

        :param id: The ID of the job to be deleted.
        :raises KeyError: if there is no job with the given key.
        :return: The removed job.
        """

        try:
            self.scheduler.remove_job( id )
        except JobLookupError:
            pass # Was stopped beforehand

        with self.transaction() as session:
            job: JobMetadata = session.query( JobMetadata ).filter_by( id = id ).first()
            if job is None:
                raise KeyError( f"There are no jobs with the id '{id}'" )
            session.delete( job )

            job_exp = job.export()
            self.output_file( job_exp ).unlink( missing_ok = True )
            return job_exp

    def get_results( self, id: str ) -> JSONData:
        """
        Retrieves the results of the job identified by the given ID.

        :param id: The ID of the job to retrieve results for.
        :return: The results of the job as a JSON object.
        :raises KeyError: if there is no job with the given key.
        """

        job = self.get_job( id )
        if job is None:
            raise KeyError( f"There are no jobs with the id '{id}'" )

        results = self.load_results( job )

        return {
            'job': job.to_json(),
            'results': results
        }