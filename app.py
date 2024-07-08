import os
import time
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from utils.process import Process
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


class PredictRequest(BaseModel):
    session_id: int
    interview_id: int


@app.get("/health")
def health():
    """
    Returns the health status of the API.
    Description: Endpoint for checking the health status of the application.
    Response: Returns a JSON object with the status "ok".
    """
    return {"status": "ok"}


@app.post("/preprocess")
async def pre_process(request: PredictRequest):
    """
    Handles preprocessing of audio data.
    Parameters: session_id (int): ID of the session.
                interview_id (int): ID of the interview.
    Returns: Returns a JSON object with the status "ok" upon successful processing.
    """
    session_id = request.session_id
    interview_id = request.interview_id

    start_time = time.time()

    process = Process(session_id, interview_id)
    process.pre_process()

    print('Preprocessing finished in {} seconds'.format(time.time() - start_time))

    return {"status": "ok"}


@app.post("/predict")
async def predict(request: PredictRequest):
    """
    Manages the complete processing and inference workflow.
    Parameters: session_id (int): ID of the session.
                interview_id (int): ID of the interview.
    Returns: Returns a JSON object with the status "ok" upon successful processing.
    """
    session_id = request.session_id
    interview_id = request.interview_id

    start_time = time.time()

    process = Process(session_id, interview_id)
    await process.process_all()

    print('Processing finished in {} seconds'.format(time.time() - start_time))

    return {"status": "ok"}


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host='0.0.0.0', port=port, reload=True)
