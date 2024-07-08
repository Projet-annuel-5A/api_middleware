# Middleware

## Overview
The Middleware module is part of the **Interviewz** application focused on processing audio interviews.

This module is responsible for :
* Audio processing, which includes tasks like diarization and speech-to-text conversion
* Communication with other APIs for further analysis from audio, text and video.

It uses asynchronous programming to handle I/O operations efficiently and FastAPI for setting up a REST API.

## Directory Structure
The module consists of several Python files organized as follows:
```plaintext
middleware/
├── app.py
├── utils/
│   ├── utils.py
```

## Components

### FastAPI Application (app.py)
Initializes a FastAPI application with CORS middleware to allow cross-origin requests from specified origins.

#### API Endpoints

```fastAPI
@app.get("/health")
"""
Returns the health status of the API. 
Description: Endpoint for checking the health status of the application.
Response: Returns a JSON object with the status "ok".
"""
```
```fastAPI
@app.post("/preprocess")
"""
Handles preprocessing of audio data.
Parameters: session_id (int): ID of the session.
            interview_id (int): ID of the interview.
Returns: Returns a JSON object with the status "ok" upon successful processing.
"""
```
```fastAPI
@app.post("/predict")
"""
Manages the complete processing and inference workflow.
Parameters: session_id (int): ID of the session.
            interview_id (int): ID of the interview.
Returns: Returns a JSON object with the status "ok" upon successful processing.
"""
```


### Utilities (utils/utils.py): 
Includes logging setup, configuration management, and methods for file operations on S3 storage.
Implements methods for updating database records and managing connections to Supabase for data storage.