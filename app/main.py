import fastapi
from app.sensors.controller import router as sensorsRouter
from yoyo import get_backend, read_migrations

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")

# Apply new TS migrations using Yoyo
#Read docs: https://ollycope.com/software/yoyo/latest/
# First get TS database
backend = get_backend("postgresql://timescale:timescale@timescale:5433/timescale")
# Get the migration to be applied, where the table is defined
migrations = read_migrations('migrations_ts')
#  Apply the migration to the TS database
with backend.lock():
    backend.apply_migrations(backend.to_apply(migrations))

app.include_router(sensorsRouter)

@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}
