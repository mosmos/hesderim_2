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
        await db.execute("CREATE TABLE IF NOT EXISTS processes (id INTEGER PRIMARY KEY, status TEXT)")
        await db.commit()


publish_hesderim_api = publish_hesderim_api.Publish_Hsederim()

def is_exists(id, environment):
    ws = publish_hesderim_api.getWorkSpace

async def long_running_process(id, environment):
    try:
        loop = asyncio.get_running_loop()
        response_object = await loop.run_in_executor(None, lambda: publish_hesderim_api.publish_hesder(id, environment))
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE processes SET status = 'done' WHERE id = ?", (id,))
            await db.commit()
        return response_object
    except Exception as ex:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE processes SET status = 'failed' WHERE id = ?", (id,))
            await db.commit()
        app.logger.error(f"Error in processing {id}: {ex}")    


####################################################
@app.route('/hesderim_2_1/', methods=['GET', 'POST'])
async def publish_hesder(sanic_req):
    try:

        id = sanic_req.args.get('id')
        environment = sanic_req.args.get('env')
        print ("@POST","env:", environment,"ID:",id)

        # check if the layer allready exists
        check_object = publish_hesderim_api.check_publish_hesder(id, environment)
        
        print (check_object)
        #return response.json({"id": id, "status": "exists"}, status=200)

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT status FROM processes WHERE id = ?", (id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    status = row[0]
                    return response.json({"id": id, "status": status}, status=200)

            await db.execute("INSERT INTO processes (id, status) VALUES (?, 'in process')", (id,))
            await db.commit()
        
        #response_object = publish_hesderim_api.publish_hesder(id, environment)
        response_object = asyncio.create_task(long_running_process(id, environment))
        #print ("@post",response_object)
        return response.json(response_object)

    except Exception as ex:
        print ('error at publish_hesder')
        return response.text(ex.args[0])







'''
@app.route('/hesderim_2_1/', methods=['GET'])
async def check_publish_hesder(sanic_req):
    try:
        id = sanic_req.args.get('id')
        environment = sanic_req.args.get('env')
        print ("@GET", "env:", environment,"ID:",id)
        
        response_object = publish_hesderim_api.check_publish_hesder(id, environment)

        if response_object['Response Code'] == 500:
            print ("@500","No Hesder found under the ID:41891")
            print ("@500","check the inprocess.DB")

        return response.json(response_object)

    except Exception as ex:
        print ('error at check_publish_hesder')
        return response.text(ex.args[0])
'''


if __name__ == '__main__':
    asyncio.run(setup_db())
    app.run(host='127.0.0.1', port=8001, debug=True,auto_reload=True)
