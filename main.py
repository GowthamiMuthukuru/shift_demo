from fastapi import FastAPI
from db import Base,engine
from app import route
app = FastAPI()
Base.metadata.create_all(bind=engine)

app.include_router(route.router)
@app.get('/')
def greet():
    return 'Welcome!'