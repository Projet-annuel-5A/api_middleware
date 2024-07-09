import av
import os
import io
import asyncio
import aiohttp
import requests
import pandas as pd
from typing import List
from utils import Utils
from pydub import AudioSegment
from dotenv import load_dotenv
from fastapi import HTTPException
from dataclasses import dataclass


@dataclass
class ApiResponse:
    identifier: str
    status: str
    content: str


class Process:
    def __init__(self, session_id: int, interview_id: int):
        load_dotenv()
        self.increasing_tqdm = False
        self.session_id = session_id
        self.interview_id = interview_id
        self.utils = Utils(session_id, interview_id)
        self.params = {
            'session_id': self.session_id,
            'interview_id': self.interview_id
        }

    def __speech_to_text(self, audio_bytes: bytes, diarization: pd.DataFrame) -> pd.DataFrame:
        """
        Converts speech segments from an audio file into text using an external API.
        Parameters:
            audio_bytes (bytes): The audio file content as bytes.
            diarization (pd.DataFrame): DataFrame containing diarization data with start and end times.
        Returns:
            pd.DataFrame: Updated DataFrame with the text obtained from speech-to-text conversion.
        Raises:
            Exception: Raises an exception if speech-to-text conversion fails.
        """
        try:
            self.utils.log.info('Starting speech to text')
            audio = AudioSegment.from_file(io.BytesIO(audio_bytes), format="mp3")
            headers = {'Authorization': 'Bearer {}'.format(os.environ.get('WHISPER_API_KEY'))}
            data = {'model': self.utils.config['SPEECHTOTEXT']['ModelId'],
                    'language': 'fr',
                    'response_format': 'text'
                    }

            for row in diarization.itertuples():
                audio_segment = audio[row.start:row.end]
                audio_segment_bytes = io.BytesIO()
                audio_segment.export(audio_segment_bytes, format="mp3")
                audio_segment_bytes.seek(0)

                file = {'file': audio_segment_bytes}
                response = requests.post(self.utils.config['SPEECHTOTEXT']['STT_API_URL'],
                                         headers=headers,
                                         files=file,
                                         data=data)
                if response.status_code == 200:
                    diarization.at[row.Index, 'text'] = response.json()

            self.utils.log.info('Speech to text done')
            return diarization
        except Exception as e:
            self.utils.log.error('An error occurred: {}'.format(e))
            raise e

    def __diarize(self, audio: bytes) -> pd.DataFrame:
        """
        Performs speaker diarization on an audio file to identify different speakers and their speech segments.
        Parameters:
            audio (bytes): The audio file content as bytes.
        Returns:
            pd.DataFrame: A DataFrame containing columns for start and end times, and speaker labels.
        Raises:
            Exception: If diarization API call fails, it logs the error and raises an exception.
        """
        try:
            self.utils.log.info('Starting diarization')
            file = {'file': io.BytesIO(audio)}
            data = {'num_speakers': '2',
                    'language': self.utils.config['GENERAL']['Language'],
                    'diarization': 'true',
                    'task': 'transcribe',
                    }
            headers = {'Authorization': 'Bearer {}'.format(os.environ.get('WHISPER_API_KEY'))}

            response = requests.post(self.utils.config['DIARIZATION']['DIARIZATION_API_URL'],
                                     headers=headers,
                                     data=data,
                                     files=file)
            df = pd.DataFrame(response.json()['diarization'])

            df.rename(columns={'startTime': 'start', 'stopTime': 'end'}, inplace=True)
            df['start'] = df['start'].map(lambda x: int(x * 1000))
            df['end'] = df['end'].map(lambda x: int(x * 1000))
            df['speaker'] = df['speaker'].map(lambda x: int(x.split('_')[1]))
            self.utils.update_bool_db('diarization_ok', True)
            self.utils.log.info('Diarization done')
            return df

        except Exception as e:
            self.utils.log.error('An error occurred: {}'.format(e))
            raise e

    def __extract_audio(self, video_path, video_name, audio_path) -> bytes:
        video_bytes = self.utils.open_input_file(video_path, video_name)

        # Create an in-memory binary stream from the byte array
        container = av.open(io.BytesIO(video_bytes))

        # Initialize an in-memory buffer to store the extracted audio
        audio_buffer = io.BytesIO()

        # Extract the audio stream
        audio_stream = next(s for s in container.streams if s.type == 'audio')

        # Create an output container for audio
        output_container = av.open(audio_buffer, mode='w', format='mp3')  # You can change the format as needed

        # Add a stream to the output container
        output_audio_stream = output_container.add_stream('mp3')

        # Process the audio frames and write them to the output container
        for frame in container.decode(audio_stream):
            packet = output_audio_stream.encode(frame)
            if packet:
                output_container.mux(packet)

        # Finalize the audio container
        output_container.close()

        # Get the extracted audio bytes
        audio_bytes = audio_buffer.getvalue()
        audio_buffer.close()

        self.utils.supabase.storage.from_(self.utils.bucket_name).upload(file=audio_bytes,
                                                                         path=audio_path,
                                                                         file_options={"content-type": "audio/mpeg"})

        return audio_bytes

    def pre_process(self) -> None:
        """
        Handles the preprocessing steps including diarization and speech-to-text
        for the given session and interview IDs.
        Raises:
            Exception: Captures and logs any exception that occurs during the preprocessing steps, then re-raises it.
        """
        print('Program started => Session: {} | Interview: {}'.format(self.session_id,
                                                                      self.interview_id))
        self.utils.log.info('Program started => Session: {} | Interview: {}'.format(self.session_id,
                                                                                    self.interview_id))
        try:
            video_name = self.utils.config['GENERAL']['Videoname']
            audio_name = self.utils.config['GENERAL']['Audioname']
            s3_path = '{}/{}/raw'.format(self.session_id, self.interview_id)
            video_path = '{}/{}'.format(s3_path, video_name)
            audio_path = '{}/{}'.format(s3_path, audio_name)

            audio_file = self.__extract_audio(video_path, video_name, audio_path)

            # Diarize and split the audio file
            diarization = self.__diarize(audio_file)
            print('Diarization done')

            results = self.__speech_to_text(audio_file, diarization)
            print('Speech to text done')

            self.utils.save_results_to_bd(results)
            print('Results saved to database')
        except Exception as e:
            print('An error occurred: {}'.format(e))
            self.utils.log.error('An error occurred: {}'.format(e))
            raise e
        finally:
            self.utils.end_logs('preprocessing')
            self.utils.__del__()

    async def __fetch(self, session, url, identifier) -> ApiResponse:
        """
        Asynchronously fetches data from a given URL using aiohttp session.
        Parameters:
            session (aiohttp.ClientSession): The session for making HTTP requests.
            url (str): The URL to which the request is to be sent.
            identifier (str): A label identifying the type of data being fetched.
        Returns:
            ApiResponse: An object containing the identifier, status, and content of the response.
        """
        try:
            async with session.post(url, params=self.params) as response:
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
                content = await response.text()
                return ApiResponse(identifier=identifier, status='ok', content=content)
        except aiohttp.ClientError as e:
            return ApiResponse(identifier=identifier, status='error', content=str(e))

    async def __call_apis(self, urls: List[str], identifiers: List[str]) -> List[ApiResponse]:
        """
        Asynchronously calls multiple APIs and collects their responses.
        Parameters:
            urls (List[str]): List of URLs to send requests to.
            identifiers (List[str]): Corresponding identifiers for each URL.
        Returns:
            List[ApiResponse]: A list of ApiResponse objects containing the responses from each API call.
        """
        async with aiohttp.ClientSession() as session:
            tasks = [self.__fetch(session, url, identifier) for url, identifier in zip(urls, identifiers)]
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            return responses

    async def process_all(self):
        """
        Manages the entire processing workflow including API calls for audio, text, and video analysis.
        Raises:
            HTTPException: On failure, logs detailed error info and raises HTTPException with status code 500.
        """
        print('Inference started => Session: {} | Interview: {}'.format(self.session_id,
                                                                        self.interview_id))
        self.utils.log.info('Inference started => Session: {} | Interview: {}'.format(self.session_id,
                                                                                      self.interview_id))
        try:
            urls = [
                'http://{}:8001/analyse_audio'.format(os.environ.get('API_AUDIO_IP')),
                'http://{}:8002/analyse_text'.format(os.environ.get('API_TEXT_IP')),
                'http://{}:8003/analyse_video'.format(os.environ.get('API_VIDEO_IP'))
            ]
            identifiers = ['audio', 'text', 'video']
            responses = await self.__call_apis(urls, identifiers)

            for response in responses:
                if response.status == 'ok':
                    column_name = response.identifier + '_ok'
                    print(f"Updating database boolean for {column_name}")
                    self.utils.update_bool_db(column_name, True)
                else:
                    self.utils.log.error(f"Error from {response.identifier}: {response.content}")

            self.utils.update_bool_db('inference_ok', True)
            self.utils.log.info('Sentiment detection from text, audio and video have finished')
            self.utils.log.info('Program finished successfully')
            print('Program finished successfully')
        except Exception as e:
            self.utils.log.error('Sentiment detection from text, audio and video have failed')
            self.utils.log.error('An error occurred: {}. Program aborted'.format(e))
            print('\n\nAn error occurred: {}. Program aborted'.format(e))
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            print('Saving log files')
            self.utils.end_logs('inference')
            self.utils.__del__()
            print('Program finished')
