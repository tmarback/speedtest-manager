import functools
import json
import logging
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, tzinfo
from pathlib import Path
from typing import Optional, Set, Sequence
from apscheduler.util import astimezone

import pytz
from apscheduler import events
from apscheduler.jobstores.base import JobLookupError, ConflictingIdError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import orm, create_engine, Column, Integer, String, TIMESTAMP, Interval, Boolean
from sqlalchemy.ext.declarative import declarative_base

from . import speedtest
from .connection import Data, JSONData

_LOGGER = logging.getLogger( __name__ )

def _date_to_str( date: Optional[datetime] ) -> Optional[str]:
    return date.astimezone( pytz.utc ).isoformat() if date is not None else None

def _str_to_date( date: Optional[str] ) -> Optional[datetime]:
    return datetime.fromisoformat( date ).astimezone( pytz.utc ) if date is not None else None

def _to_timezone( date: Optional[datetime], tz: tzinfo ) -> Optional[datetime]:
    return date.astimezone( tz ) if date is not None else None

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
    created: Optional[datetime] = None

    def __post_init__( self ):

        if self.id is None:
            raise ValueError( "The job ID must be specified." )
        if self.server_id is None and self.server_name is None:
            raise ValueError( "Either the server ID or hostname must be specified." )
        if self.interval is not None and self.interval.total_seconds() < 1:
            raise ValueError( "The interval must be either at least one second or None." )

    def to_json( self ) -> JSONData:

        return {
            'id': self.id,
            'title': self.title,
            'server_id': self.server_id,
            'server_name': self.server_name,
            'interval': int( self.interval.total_seconds() ) if self.interval is not None else None,
            'start': _date_to_str( self.start ),
            'end': _date_to_str( self.end ),
            'running': self.running,
            'created': _date_to_str( self.created ),
        }

    @classmethod
    def from_json( cls, data: JSONData ) -> 'Job':

        try:
            return cls(
                id = data['id'],
                title = data['title'],
                server_id = data['server_id'],
                server_name = data['server_name'],
                interval = timedelta( seconds = data['interval'] ) if data['interval'] is not None else None,
                start = _str_to_date( data['start'] ),
                end = _str_to_date( data['end'] ),
                running = data['running'],
                created = _str_to_date( data['created'] ),
            )
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
    start = Column( TIMESTAMP( timezone = True ) )
    end = Column( TIMESTAMP( timezone = True ) )
    running = Column( Boolean, nullable = False, default = True )
    created = Column( TIMESTAMP( timezone = True ), nullable = False, default = functools.partial( datetime.now, pytz.utc ) )

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
            start = _to_timezone( job.start, pytz.utc ),
            end = _to_timezone( job.end, pytz.utc ),
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
            running = self.running,
            created = self.created,
        )

    @orm.reconstructor
    def insert_timezone( self ):
        """
        Ensures all timestamps have timezones attached.
        """

        if self.start is not None:
            self.start = self.start.replace( tzinfo = pytz.utc )
        if self.end is not None:
            self.end = self.end.replace( tzinfo = pytz.utc )
        self.created = self.created.replace( tzinfo = pytz.utc )

class JobError( RuntimeError ):
    """
    Base type of exceptions related to job configuration.
    """
    pass

class IDExistsError( JobError ):
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

class PastEndError( JobError ):
    """
    Exception that indicates an attempt to schedule an interval-based job with an
    end date in the past.
    """

    def __init__( self, now: datetime, job: Job ):
        """
        Creates a new instance.

        :param now: The current time used for comparison.
        :param job: The job that was to be scheduled.
        """

        super().__init__( f"Job '{job.id}' has end date ({job.end}) in the past (current time is {now})." )
        self._now = now
        self._job = job

    @property
    def now( self ) -> datetime:
        """
        The current time at the moment the error was generated.
        """

        return self._now

    @property
    def id( self ) -> str:
        """
        The ID of the job that triggered this error.
        """
        return self._job.id

    @property
    def end( self ) -> datetime:
        """
        The end time of the job that triggered this error.
        """

        return self._job.end

class JobManager:
    """
    Central overseer that manages the measurement jobs.
    """

    _instance: Optional['JobManager'] = None

    @classmethod
    def initialize( cls, datadir: Path ) -> 'JobManager':

        if cls._instance is None:
            cls._instance = JobManager( datadir )
        return cls._instance

    @classmethod
    def get_instance( cls ) -> 'JobManager':

        if cls._instance is None:
            raise RuntimeError( "Attempted to obtain manager before initializing it." )
        return cls._instance

    @classmethod
    def run_job( cls, job: Job ) -> None:
        """
        Executes the specified job once.

        :param job: The job to execute.
        """

        _LOGGER.debug( "Running job '%s'.", job.id )

        timestamp = datetime.now( pytz.utc )
        try:
            output = speedtest.run_test( server_id = job.server_id, server_name = job.server_name )
            result = {
                'success': True,
                'time': timestamp.isoformat(),
                'result': output
            }
        except speedtest.TestError as e:
            _LOGGER.exception( "Test could not be completed." )
            result = {
                'success': False,
                'timestamp': timestamp.isoformat(),
                'error': str( e ),
                'stdout': e.stdout,
                'stderr': e.stderr,
            }

        with open( cls.get_instance().output_file( job ), 'a' ) as f:
            f.write( json.dumps( result ) )
            f.write( ',\n' ) # Line break to make it slightly more readable

        _LOGGER.debug( "Finished running job '%s'.", job.id )

    def __init__( self, datadir: Path ):
        """
        Initializes a new manager that uses the specified directory to store data.

        :param datadir: The path of the directory where data should be stored.
        """

        _LOGGER.debug( "Initializing manager." )
        try:
            _LOGGER.info( "Using %s", speedtest.get_version() ) # Also implicitly check installed
        except speedtest.TestError:
            _LOGGER.exception( "Obtaining Speedtest CLI version caused an error." )
            _LOGGER.critical( "The Speedtest CLI could not accessed. Is it installed in this system?" )
            sys.exit( 1 )

        database_path = datadir / 'jobs.db'

        self.storage = datadir / 'results'
        self.storage.mkdir( mode = 0o770, exist_ok = True )

        self.engine = create_engine( f'sqlite:///{database_path}' )
        Base.metadata.create_all( self.engine )
        self.Session = orm.sessionmaker( bind = self.engine )

        jobstores = {
            'default': SQLAlchemyJobStore( engine = self.engine )
        }
        executors = {
            'default': ThreadPoolExecutor( 1 )
        }
        job_defaults = {
            'coalesce': True,
            'max_instances': 1,
            'misfire_grace_time': 5 * 60, # Can be up to 5 minutes late
        }
        self.scheduler = BackgroundScheduler( jobstores = jobstores, executors = executors, job_defaults = job_defaults, timezone = pytz.utc )
        self.scheduler.add_listener( self.job_stopped, mask = events.EVENT_JOB_REMOVED )

        _LOGGER.debug( "Manager initialized." )

    def start( self ):
        """
        Starts processing jobs.
        """

        _LOGGER.info( "Manager starting." )
        self.scheduler.start()
        _LOGGER.debug( "Manager started." )

    def shutdown( self, wait: bool = True ):
        """
        Shuts down the manager, stopping job processing.

        :param wait: If True, waits for all currently executing jobs to finish before returning.
        """

        _LOGGER.info( "Manager stopping." )
        self.scheduler.shutdown( wait = wait )
        _LOGGER.debug( "Manager stopped." )

    def job_stopped( self, event: events.JobEvent ) -> None:

        id: str = event.job_id
        with self.transaction() as session:
            job: JobMetadata = session.query( JobMetadata ).filter_by( id = id ).first()
            job.running = False

    def output_file( self, job: Job ) -> Path:
        """
        Determines the path to the output file of the job identified by the given ID.

        :param job: The job to get the path for.
        :return: The path of the output file for the given job.
        """

        return self.storage / f'{job.id}.result' # Not really proper JSON

    def load_results( self, job: Job ) -> Sequence[JSONData]:
        """
        Loads the results obtained so far for the given job.

        :param job: The job to load results for.
        :return: The results of the given job, as a list of JSON objects.
        """

        output_file = self.output_file( job )
        if not output_file.exists():
            return []

        with open( output_file, 'r' ) as f:
            results = f.read()
        results = '[' + results[:-2] + ']' # Remove trailing comma and line break and add brackets

        return json.loads( results )

    @contextmanager
    def transaction( self ) -> orm.Session:
        """
        Provide a transactional scope around a series of operations.
        """

        session: orm.Session = self.Session()
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

        _LOGGER.info( "Registering job '%s'.", job.id )
        _LOGGER.debug( "Job '%s' (%s) has target %d|'%s', starts at %s and ends at %s with interval %s.",
            job.id, job.title, job.server_id, job.server_name, job.start, job.end, job.interval
        )
        with self.transaction() as session:
            try:
                if session.query( JobMetadata ).filter_by( id = job.id ).count() > 0:
                    raise IDExistsError( "There is already metadata for the given ID." )

                new_job = JobMetadata( job )
                session.add( new_job )

                if job.interval:
                    _LOGGER.debug( "Creating an interval-triggered job." )
                    if job.end is not None and job.end < ( now := datetime.now( pytz.utc ) ):
                        raise PastEndError( now, job )
                    trigger = IntervalTrigger( 
                        seconds = int( job.interval.total_seconds() ),
                        start_date = job.start if job.start is not None else datetime.now( pytz.utc ),
                        end_date = job.end
                    )
                else:
                    _LOGGER.debug( "Creating a date-triggered job." )
                    trigger = DateTrigger(
                        run_date = job.start
                    )

                self.scheduler.add_job( 
                    func = JobManager.run_job,
                    args = [ job ],
                    trigger = trigger,
                    id = job.id,
                    name = job.title if job.title else job.id,
                )
            except ( IDExistsError, ConflictingIdError ) as e:
                _LOGGER.debug( "Attempted to register duplicate ID '%s'.", job.id )
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
            _LOGGER.debug( "Attempted to stop job ID '%s', but it is not running.", id )
            raise KeyError( f"There are no currently running jobs with the id '{id}'" )
        return self.get_job( id )

    def delete_job( self, id: str ) -> Job:
        """
        Removes the job with the given ID from the manager's database. This will stop the job if is
        currently executing, and the job's output is also removed.

        :param id: The ID of the job to be deleted.
        :raises KeyError: if there is no job with the given key.
        :return: The removed job.
        """

        try:
            self.stop_job( id )
        except KeyError:
            _LOGGER.debug( "Deleting job that was already stopped '%s'.", id )
            pass # Was stopped beforehand

        with self.transaction() as session:
            job: JobMetadata = session.query( JobMetadata ).filter_by( id = id ).first()
            if job is None:
                _LOGGER.debug( "Attempted to delete job with non-existent ID '%s'.", id )
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
            _LOGGER.debug( "Attempted to get results of job with non-existent ID '%s'.", id )
            raise KeyError( f"There are no jobs with the id '{id}'" )

        results = self.load_results( job )

        return {
            'job': job.to_json(),
            'results': results
        }