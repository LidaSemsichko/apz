from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

# {
#   "logging-service": ["http://logging-service-1:8000", ...],
#   "counter-service": ["http://counter-service:8000"]
# }
registry: dict[str, list[str]] = {}


class RegisterRequest(BaseModel):
    service_name: str
    service_url: str


@app.get("/health")
def health():
    return {"status": "ok", "service": "config-service"}


@app.post("/register")
def register_service(req: RegisterRequest):
    if req.service_name not in registry:
        registry[req.service_name] = []

    if req.service_url not in registry[req.service_name]:
        registry[req.service_name].append(req.service_url)

    print(f"[config-service] Registered {req.service_name} -> {req.service_url}")
    return {
        "status": "registered",
        "service_name": req.service_name,
        "service_url": req.service_url,
    }


@app.get("/services/{service_name}")
def get_service_instances(service_name: str):
    instances = registry.get(service_name, [])
    return {
        "service_name": service_name,
        "instances": instances,
    }


@app.get("/registry")
def get_registry():
    return registry