from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Hello World"}

@app.get("/items/{item_id}")
def item(item_id: str):
    return {"item_id": item_id}
import numpy as np
@app.get("/rand/{max}")
def rand(min: int = 0, max: int = 10):
    return f'Random number: {np.random.randint(min+1, max+1)}'

