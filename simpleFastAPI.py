"""
A minimal "Hello World" FastAPI application.
This serves as a basic example or a health check endpoint.
"""
from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}