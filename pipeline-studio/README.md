# NL2Pipeline Project Execution Guide

This document provides the standard operating procedures for setting up and running the local development environment for the NL2Pipeline studio.

---

## Workspace Initialization

Running the application requires executing the frontend and backend services simultaneously in separate terminal windows or panels.

### Panel 1: Frontend Deployment

Execute the following commands to initialize and launch the user interface server:

cd pipeline-studio
npm install
npm run dev

The frontend user interface will be served and available at: http://localhost:5173/

Panel 2: Backend Deployment

Open a new terminal window or tab panel, then execute the following commands to spin up the local API instance:


cd pipeline-studio/backend
pip install fastapi uvicorn
uvicorn main:app --port 8000 --reload

The backend API proxy will initialize and listen for incoming handshake requests on port 8000.

Production Dependencies
To install the exact Python package dependencies required for the backend infrastructure, run the following installation command from the directory containing the project's configuration files:

pip install -r requirements.txt