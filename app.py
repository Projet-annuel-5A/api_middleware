import os
import io
import asyncio
import aiohttp
import uvicorn
import requests
import pandas as pd
from typing import List
from utils.utils import Utils
from pydantic import BaseModel
from pydub import AudioSegment
from dotenv import load_dotenv
from dataclasses import dataclass
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()

origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@dataclass
class ApiResponse:
    identifier: str
    status: str
    content: str


class PredictRequest(BaseModel):
    session_id: int
    interview_id: int


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

    def pre_process(self) -> None:
        print('Program started => Session: {} | Interview: {}'.format(self.session_id,
                                                                      self.interview_id))
        self.utils.log.info('Program started => Session: {} | Interview: {}'.format(self.session_id,
                                                                                    self.interview_id))
        try:
            filename = self.utils.config['GENERAL']['Audioname']
            s3_path = '{}/{}/raw/{}'.format(self.session_id, self.interview_id, filename)
            audio_file = self.utils.open_input_file(s3_path, filename)

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

    async def __fetch(self, session, url, identifier):
        try:
            async with session.post(url, params=self.params) as response:
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
                content = await response.text()
                return ApiResponse(identifier=identifier, status='ok', content=content)
        except aiohttp.ClientError as e:
            return ApiResponse(identifier=identifier, status='error', content=str(e))

    async def __call_apis(self, urls: List[str], identifiers: List[str]) -> List[ApiResponse]:
        async with aiohttp.ClientSession() as session:
            tasks = [self.__fetch(session, url, identifier) for url, identifier in zip(urls, identifiers)]
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            return responses

    async def process_all(self):
        print('Inference started => Session: {} | Interview: {}'.format(self.session_id,
                                                                        self.interview_id))
        self.utils.log.info('Inference started => Session: {} | Interview: {}'.format(self.session_id,
                                                                                      self.interview_id))
        try:
            urls = [
                'http://{}:8001/analyse_audio'.format(os.environ.get('API_AUDIO_IP')),
                'http://{}:8004/analyse_text'.format(os.environ.get('API_TEXT_IP')),
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


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/preprocess")
async def pre_process(request: PredictRequest):
    session_id = request.session_id
    interview_id = request.interview_id

    process = Process(session_id, interview_id)
    process.pre_process()

    return {"status": "ok"}


@app.post("/predict")
async def predict(request: PredictRequest):
    session_id = request.session_id
    interview_id = request.interview_id

    process = Process(session_id, interview_id)
    await process.process_all()
    return {"status": "ok"}


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host='0.0.0.0', port=port, reload=True)
