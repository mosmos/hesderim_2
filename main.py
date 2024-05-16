from sanic import Sanic, response
from sanic_cors import CORS
from sanic import request as rq
import publish_hesderim_api

app = Sanic("testppr21")
CORS(app, resources=r'/*', origins="*",
     methods=["GET", "POST","DELETE", "HEAD", "OPTIONS"])
app.config.PROXIES_COUNT = 1  # Set the number of trusted proxy servers

publish_hesderim_api = publish_hesderim_api.Publish_Hsederim()

def is_exists(id, environment):
    ws = publish_hesderim_api.getWorkSpace

@app.route('/hesderim_2_1/', methods=['POST'])
async def publish_hesder(rq):
    try:
        id = rq.args.get('id')
        environment = rq.args.get('env')
        print ("@POST","env:", environment,"ID:",id)
        
        response_object = publish_hesderim_api.publish_hesder(id, environment)
        #print ("@post",response_object)
        return response.json(response_object)

    except Exception as ex:
        print ('error at publish_hesder')
        return response.text(ex.args[0])


@app.route('/hesderim_2_1/', methods=['GET'])
async def check_publish_hesder(rq):
    try:
        id = rq.args.get('id')
        environment = rq.args.get('env')
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
@app.route('/nativ/', methods=['GET', 'POST'])
async def copy_rishui(rq):
    try:
        print ("nativ")
        id_koma = rq.args.get('id_koma')
        environment = rq.args.get('env')
        response_object, response_code = publish_hesderim_api.copy_rishui(id_koma, environment)

        return response.json(response_object )

    except Exception as ex:
        return response.text(ex.args[0])
        
        
@app.route('/nativ/', methods=['DELETE'])
async def nativDeleteRaster(rq):
    try:
        id_koma = rq.args.get('id_koma')
        environment = rq.args.get('env')
        key = rq.args.get('key')
        response_object, response_code = publish_hesderim_api.nativDeleteRaster(id_koma, environment, key)

        return response.json(response_object )

    except Exception as ex:
        return response.text(ex.args[0])
        
        
@app.route('/nativ_token/', methods=['GET', 'POST'])
def verifyNativ(rq):
    try:
        user = rq.args.get('user')
        password = rq.args.get('password')
        response_object, response_code = publish_hesderim_api.verifyNativ(
            user, password)

        return create_response(response_object )
    except Exception as ex:
        return response.text(ex.args[0])
'''

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8001, auto_reload=True)
