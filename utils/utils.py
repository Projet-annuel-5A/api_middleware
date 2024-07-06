import os
import sys
import logging
import tempfile
import configparser
import pandas as pd
from typing import Any
from datetime import datetime
from supabase import create_client, Client


# Create custom stream handler
class LoggerWriter:
    def __init__(self, logger, level) -> None:
        self.level = level
        self.logger = logger

    def write(self, message: str) -> None:
        if message.strip():
            self.logger.log(self.level, message.strip())

    def flush(self) -> None:
        pass


class BufferingHandler(logging.Handler):
    def __init__(self, filename: str) -> None:
        super().__init__()
        self.buffer = []
        self.filename = filename

    def emit(self, record: logging.LogRecord) -> None:
        # Append the log record to the buffer
        self.buffer.append(self.format(record))

    def flush(self) -> str:
        if len(self.buffer) > 0:
            return '\n'.join(self.buffer)
        else:
            return ''


class Utils:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

    def __init__(self, session_id: int = None, interview_id: int = None) -> None:
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
        log_handlers = logging.getLogger('mainLog').handlers[:]
        for handler in log_handlers:
            if isinstance(handler, BufferingHandler):
                log = handler.flush()
                if log:
                    self.save_to_s3('{}_{}.log'.format(name, handler.filename), log.encode(), 'text', 'logs')
            logging.getLogger('mainLog').removeHandler(handler)

    def __check_supabase_connection(self) -> Client:
        try:
            client = create_client(self.config['SUPABASE']['Url'], os.environ.get('SUPABASE_KEY'))
        except Exception as e:
            message = ('Error connecting to Supabase, the program can not continue.', str(e))
            self.log.error(message)
            print(message)
            sys.exit(1)
        return client

    def __connect_to_bucket(self) -> Any:
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
        self.log.info('Updating {} to {} in the database'.format(champ_name, value))
        try:
            self.supabase.table('interviews').update({champ_name: value}).eq('id', self.interview_id).execute()
            self.log.info('{} updated successfully to {}'.format(champ_name, value))
        except Exception as e:
            message = ('Error updating {} in the database'.format(champ_name), str(e))
            self.log.error(message)

    """
    def df_to_temp_s3(self, df: pd.DataFrame, filename: str) -> None:
        s3_path = '{}/temp/{}.tmp'.format(self.output_s3_folder, filename)
        # Get the temporary directory path
        temp_dir = tempfile.gettempdir()

        # Create the full path for the temporary file
        temp_file_path = os.path.join(temp_dir, '{}.h5'.format(filename))

        with open(temp_file_path, 'w') as temp_file:
            df.to_hdf(temp_file_path, key='data', mode='w')

        with open(temp_file_path, 'rb') as f:
            try:
                self.supabase_connection.upload(file=f, path=s3_path,
                                                file_options={'content-type': 'application/octet-stream'})
                self.log.info('File {} uploaded to S3 bucket'.format(s3_path))
            except Exception as e:
                message = (
                    'Error uploading the file to the S3 bucket. ', str(e))
                self.log.info(message)
                print(message)

        temp_file.close()
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
    """

    def __read_df_from_s3(self, filename) -> pd.DataFrame:
        path = '{}/temp/{}.tmp'.format(self.output_s3_folder, filename)
        df = pd.DataFrame()
        with tempfile.NamedTemporaryFile(suffix='.h5', delete=False) as temp_file:
            temp_file_path = temp_file.name
            try:
                res = self.supabase_connection.download(path)
                temp_file.write(res)
                df = pd.read_hdf(temp_file_path, key='data', index_col=None)
            except Exception as e:
                message = ('Error reading the dataframe {} from the S3. '.format(filename), str(e))
                self.log.info(message)
                print(message)
            finally:
                temp_file.close()
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
        return df

    def save_results_to_bd(self, results: pd.DataFrame) -> None:
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
