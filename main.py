from sanic import Sanic, response
from sanic_cors import CORS
from sanic import request as sanic_req
import aiosqlite
import asyncio

import publish_hesderim_api

app = Sanic(__name__)
CORS(app, resources=r'/*', origins="*",
     methods=["GET", "POST","DELETE", "HEAD", "OPTIONS"])
app.config.PROXIES_COUNT = 1  # Set the number of trusted proxy servers


DB_PATH = 'inprocesses.db'

async def setup_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS processes (id INTEGER PRIMARY KEY, job_status TEXT)")
        await db.commit()


publish_hesderim_api = publish_hesderim_api.Publish_Hsederim()

def is_exists(id, environment):
    ws = publish_hesderim_api.getWorkSpace

async def long_running_process(id, environment):
    try:
        loop = asyncio.get_running_loop()
        response_object = await loop.run_in_executor(None, lambda: publish_hesderim_api.publish_hesder(id, environment))
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE processes SET job_status = 'done' WHERE id = ?", (id,))
            await db.commit()
        return response_object
    except Exception as ex:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE processes SET job_status = 'failed' WHERE id = ?", (id,))
            await db.commit()
        app.logger.error(f"Error in processing {id}: {ex}")    


####################################################
@app.route('/hesderim_2_1/', methods=['GET'])
async def publish_hesder(sanic_req):
    try:

        id = sanic_req.args.get('id')
        environment = sanic_req.args.get('env')

        if not id:
            return response.json({"error": "id is required"}, status=400)
    
        print ("@ publish:","env:", environment,"ID:",id)

        # check if the layer allready exists
        check_object = publish_hesderim_api.check_publish_hesder(id, environment)

        if check_object['Response Code']==304:
            print ("@ allready exists , delete row","ID:",id)
            # DELETE FROM processes WHERE id = 1;
            '''async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("DELETE FROM processes WHERE id = ?", (id)):
                    await db.commit()'''
            return  response.json(check_object,status=200)
        
        # if the layer is not exists or the taster is not exists 
        # than we have to create it and register the status to sqlite.DB
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT job_status FROM processes WHERE id = ?", (id)) as cursor:
                row = await cursor.fetchone()
                if row:
                    job_status = row[0]
                    return response.json({"id": id, "message": job_status}, status=201)

            await db.execute("INSERT INTO processes (id, job_status) VALUES (?, 'in process')", (id,))
            await db.commit()
        
        # creating the task that will create the raster and publish the layer
        asyncio.create_task(long_running_process(id, environment))

        return response.json({"id": id, "message": "started"}, status=202)

    except Exception as ex:
        print ('error at publish_hesder',ex)
        return response.json({"id": id, "message": ex}, status=500)



if __name__ == '__main__':
    asyncio.run(setup_db())
    app.run(host='127.0.0.1', port=8001, auto_reload=True)
