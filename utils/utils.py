import os
import sys
import logging
import configparser
import pandas as pd
from typing import Any
from datetime import datetime
from supabase import create_client, Client


# Create custom stream handler
class LoggerWriter:
    """
    Custom writer that redirects stdout or stderr to a specified logger. This class is useful for capturing
    print statements or other output and redirecting them to a log file.
    Attributes:
        level (int): The logging level (e.g., logging.INFO, logging.ERROR) for the messages.
        logger (logging.Logger): The logger instance to which the messages are directed.
    """
    def __init__(self, logger, level) -> None:
        """
        Initializes the LoggerWriter with a specified logger and logging level.
        Parameters:
            logger (logging.Logger): The logger instance to which messages will be directed.
            level (int): The logging level for the messages.
        """
        self.level = level
        self.logger = logger

    def write(self, message: str) -> None:
        """
        Writes a message to the logger. If the message is not empty or whitespace, it is logged at the specified level.
        Parameters:
            message (str): The message to be logged.
        """
        if message.strip():
            self.logger.log(self.level, message.strip())

    def flush(self) -> None:
        pass


class BufferingHandler(logging.Handler):
    """
    Custom logging handler that buffers log records in memory. This handler is useful for situations where
    logs need to be accumulated and processed in bulk rather than being written out individually.
    Attributes:
        buffer (list): A list to hold formatted log records.
        filename (str): The name of the log file for which this handler is created.
    """
    def __init__(self, filename: str) -> None:
        """
        Initializes the BufferingHandler with a specified filename.
        Parameters:
            filename (str): The name of the log file associated with this handler.
        """
        super().__init__()
        self.buffer = []
        self.filename = filename

    def emit(self, record: logging.LogRecord) -> None:
        """
        Formats and appends a log record to the buffer.
        Parameters:
            record (logging.LogRecord): The log record to be processed and added to the buffer.
        """
        # Append the log record to the buffer
        self.buffer.append(self.format(record))

    def flush(self) -> str:
        """
        Flushes the buffer by joining all buffered log records into a single string. Clears the buffer afterward.
        Returns:
            str: A single string containing all buffered log records separated by newlines.
                  Returns an empty string if the buffer is empty.
        """
        if len(self.buffer) > 0:
            return '\n'.join(self.buffer)
        else:
            return ''


class Utils:
    """
    Provides utility functions and classes for logging, configuration management, and interaction with cloud storage.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

    def __init__(self, session_id: int = None, interview_id: int = None) -> None:
        """
        Parameters:
            session_id (int)
            interview_id (int)
        Functionality:
            Initializes logging, configuration, and database client (supabase_client).
        """
        if not self.__initialized:
            self.config = self.__get_config()

            self.session_id = session_id
            self.interview_id = interview_id

            # S3 Folders
            self.output_s3_folder = '{}/{}/output'.format(self.session_id, self.interview_id)

            # Create loggers
            self.log = self.__init_logs()
            self.log.propagate = False

            self.supabase_client = self.__check_supabase_connection()
            self.supabase_connection = self.__connect_to_bucket()
            self.supabase: Client = create_client(self.config['SUPABASE']['Url'], os.environ.get('SUPABASE_KEY'))

            self.__initialized = True

    def __del__(self):
        self.__initialized = False

    def __init_logs(self) -> logging.Logger:
        """
        Initializes and configures logging for the application. This method sets up separate log handlers
        for INFO and ERROR level messages to ensure logs are captured appropriately.
        Returns:
            logging.Logger: The configured root logger with handlers for INFO and ERROR logs.
        Functionality:
            - Sets logging level to INFO for general logs.
            - Configures formatters to include timestamp, log level, and message details.
            - Creates separate file handlers for INFO and ERROR logs with buffering capabilities.
        """
        root_logger = logging.getLogger('mainLog')
        root_logger.setLevel(logging.INFO)

        # Configure basic logging settings
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s')
        date_format = '%d/%b/%Y %H:%M:%S'
        encoding = 'utf-8'

        # Create a file handler for INFO messages
        info_log = 'log_{}'.format(datetime.now().strftime('%Y_%m_%d_%H.%M.%S'))
        info_handler = BufferingHandler(info_log)
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(formatter)

        # Create a file handler for ERROR messages
        error_log = 'errorLog_{}'.format(datetime.now().strftime('%Y_%m_%d_%H.%M.%S'))
        error_handler = BufferingHandler(error_log)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)

        root_logger.handlers.clear()

        # Add the handlers to the root logger
        root_logger.addHandler(info_handler)
        root_logger.addHandler(error_handler)
        root_logger.datefmt = date_format
        root_logger.encoding = encoding
        return root_logger

    def __get_config(self) -> configparser.ConfigParser:
        """
        Loads and returns the configuration settings from a 'config.ini' file. This method ensures that the application
        configuration is centrally managed and easily accessible.
        Returns:
            configparser.ConfigParser: A configuration parser object loaded with settings from 'config.ini'.
        Raises:
            IOError: If 'config.ini' is not found, raises an IOError and halts the program, indicating the dependency
                     on this configuration file for the application's operation.
        """
        config = configparser.ConfigParser()
        if len(config.sections()) == 0:
            try:
                base_path = os.path.dirname(os.path.dirname(__file__))
                path = os.path.join(base_path, 'config', 'config.ini')
                with open(path) as f:
                    config.read_file(f)
            except IOError as e:
                print("No file 'config.ini' is present, the program can not continue")
                raise e
        return config

    def end_logs(self, name) -> None:
        """
        Functionality:
            Flushes buffered logs to a file and uploads it to S3.
            Ends logging for the session.
        """
        log_handlers = logging.getLogger('mainLog').handlers[:]
        for handler in log_handlers:
            if isinstance(handler, BufferingHandler):
                log = handler.flush()
                if log:
                    self.save_to_s3('{}_{}.log'.format(name, handler.filename), log.encode(), 'text', 'logs')
            logging.getLogger('mainLog').removeHandler(handler)

    def __check_supabase_connection(self) -> Client:
        """
        Attempts to establish a connection with the Supabase client using the application's configuration settings.
        Returns:
            Client: The connected Supabase client if the connection is successful.
        Raises:
            Exception: Logs and raises an exception if the connection to Supabase fails, including error details.
                       This method will also terminate the application (sys.exit(1)) if a connection cannot be
                       established, as the connection is critical for the application's functionality.
        """
        try:
            client = create_client(self.config['SUPABASE']['Url'], os.environ.get('SUPABASE_KEY'))
        except Exception as e:
            message = ('Error connecting to Supabase, the program can not continue.', str(e))
            self.log.error(message)
            print(message)
            sys.exit(1)
        return client

    def __connect_to_bucket(self) -> Any:
        """
        Establishes and returns a connection to a designated S3 bucket using the Supabase client.
        This method is essential for managing file storage operations within the application.
        Returns:
            Any: The connection object to the designated S3 bucket if the connection is successful.
        Raises:
            Exception: Logs an error and terminates the application if the connection to the S3 bucket fails.
                       This ensures that the application does not continue without necessary storage capabilities.
        """
        bucket_name = self.config['SUPABASE']['InputBucket']
        connection = self.supabase_client.storage.from_(bucket_name)
        try:
            connection.list()
            self.log.info('Connection to S3 bucket {} successful'.format(bucket_name))
        except Exception as e:
            message = ('Error connecting to S3 bucket {}, the program can not continue.'.
                       format(bucket_name), str(e))
            self.log.error(message)
            print(message)
            sys.exit(1)
        return connection

    def save_to_s3(self, filename: str, content: Any, file_format: str, s3_subfolder: str = None) -> None:
        """
       Saves a file to an S3 bucket.
       Parameters:
           filename (str): The name of the file to save.
           content (Any): The content of the file.
           file_format (str): The format of the file (audio, video, text).
           s3_subfolder (str): Optional. The subfolder within the S3 bucket to save the file.
       Raises:
           Exception: Raises an exception if the file upload fails.
       """
        match file_format:
            case 'audio':
                content_type = 'audio/mpeg'
            case 'video':
                content_type = 'video/mp4'
            case 'text':
                content_type = 'text/plain'
            case _:
                content_type = 'text/plain'

        try:
            s3_path = '{}/{}/{}'.format(self.output_s3_folder,
                                        s3_subfolder,
                                        filename) if s3_subfolder else '{}/{}'.format(self.output_s3_folder, filename)
            self.supabase_connection.upload(file=content, path=s3_path, file_options={'content-type': content_type})
            self.log.info('File {} uploaded to S3 bucket at {}'.format(filename, s3_path))
        except Exception as e:
            message = ('Error uploading the file {} to the S3 bucket.'.
                       format(filename), str(e))
            self.log.error(message)

    def open_input_file(self, s3_path: str, file_name: str) -> bytes | None:
        """
        Opens and reads a file from S3 storage.
        Parameters:
            s3_path (str): Path in the S3 bucket where the file is stored.
            file_name (str): Name of the file to be retrieved.
        Returns:
            bytes | None: The content of the file as bytes, or None if an error occurs.
        Raises:
            Exception: Logs and raises an exception if file retrieval fails.
        """
        try:
            self.log.info('Getting file {} from the S3 bucket'.format(file_name))
            file_bytes = self.supabase_connection.download(s3_path)
            return file_bytes
        except Exception as e:
            message = ('Error downloading the file {} from the S3 bucket. '.
                       format(file_name), str(e))
            self.log.error(message)
            raise e

    def update_bool_db(self, champ_name: str, value: bool) -> None:
        """
        Updates a boolean value in the database for a given field name.
        Parameters:
            champ_name (str): The field name in the database to update.
            value (bool): The boolean value to set for the specified field.
        Raises:
            Exception: Logs and raises an exception if the database update fails.
        """
        self.log.info('Updating {} to {} in the database'.format(champ_name, value))
        try:
            self.supabase.table('interviews').update({champ_name: value}).eq('id', self.interview_id).execute()
            self.log.info('{} updated successfully to {}'.format(champ_name, value))
        except Exception as e:
            message = ('Error updating {} in the database'.format(champ_name), str(e))
            self.log.error(message)

    def save_results_to_bd(self, results: pd.DataFrame) -> None:
        """
        Save the results to the Supabase database.
        Parameters:
            results (pd.DataFrame): The data to save to the database.
        Raises:
            Exception: Logs and raises an exception if the database update fails.
        """
        self.log.info('Saving results to the supabase database')

        try:
            response = self.supabase.table('interviews').select('user_id').eq('id', self.interview_id).execute()
            user_id = response.data[0]['user_id']

            results['interview_id'] = self.interview_id
            results['user_id'] = user_id
            results = results.fillna('')

            data_to_insert = results.to_dict(orient='records')

            response = self.supabase.table('results').insert(data_to_insert).execute()
            self.log.info('{} lines saved to the database successfully'. format(len(response.data)))
        except Exception as e:
            self.log.error('Error saving results to the database', str(e))
            raise e
