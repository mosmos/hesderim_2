from sanic import Sanic, response
import aiosqlite
import asyncio

app = Sanic("ProcessApp")
DB_PATH = 'processes.db'

async def setup_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS processes (id INTEGER PRIMARY KEY, status TEXT)")
        await db.commit()

def run_task(_id):
    import time
    time.sleep(_id)  # Blocking call simulating a long task

async def long_running_process(_id):
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, run_task, _id)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE processes SET status = 'done' WHERE id = ?", (_id,))
            await db.commit()
    except Exception as ex:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE processes SET status = 'failed' WHERE id = ?", (_id,))
            await db.commit()
        app.logger.error(f"Error in processing {_id}: {ex}")

@app.post("/process/")
async def start_process(request):
    _id = int(request.args.get('_id'))
    if not _id:
        return response.json({"error": "_id is required"}, status=400)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT status FROM processes WHERE id = ?", (_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                status = row[0]
                return response.json({"task_id": _id, "status": status}, status=200)

        await db.execute("INSERT INTO processes (id, status) VALUES (?, 'in process')", (_id,))
        await db.commit()

    asyncio.create_task(long_running_process(_id))
    return response.json({"message": "Process started", "id": _id}, status=202)

@app.get("/process/")
async def get_status(request):
    _id = int(request.args.get('_id'))
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT status FROM processes WHERE id = ?", (_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                status = row[0]
            else:
                status = "No such process"
    return response.json({"id": _id, "status": status})

if __name__ == "__main__":
    asyncio.run(setup_db())
    app.run(host="0.0.0.0", port=8000, debug=True, auto_reload=True)
