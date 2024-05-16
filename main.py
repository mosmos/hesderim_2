from sanic import Sanic, response
from sanic_cors import CORS
from sanic import request as rq
import publish_hesderim_api

app = Sanic(__name__)
CORS(app, resources=r'/*', origins="*",
     methods=["GET", "POST","DELETE", "HEAD", "OPTIONS"])
app.config.PROXIES_COUNT = 1  # Set the number of trusted proxy servers

publish_hesderim_api = publish_hesderim_api.Publish_Hsederim()


def create_response(response_object):
    return response.json(response_object)
    # r.mimetype = "application/json"
    # r.status = response_code
    # r.headers.add('Access-Control-Allow-Origin', '*')
    # return r


@app.route('/hesderim_2/', methods=['GET', 'POST'])
async def publish_hesder(rq):
    try:
        id = rq.args.get('id')
        environment = rq.args.get('env')
        print ("env:", environment,"ID:",id)
        
        response_object = publish_hesderim_api.publish_hesder(id, environment)
        #print (response_object)
        return create_response(response_object)

    except Exception as ex:
        return response.text(ex.args[0])


@app.route('/nativ/', methods=['GET', 'POST'])
async def copy_rishui(rq):
    try:
        print ("nativ")
        id_koma = rq.args.get('id_koma')
        environment = rq.args.get('env')
        response_object, response_code = publish_hesderim_api.copy_rishui(id_koma, environment)

        return create_response(response_object )

    except Exception as ex:
        return response.text(ex.args[0])
        
        
@app.route('/nativ/', methods=['DELETE'])
async def nativDeleteRaster(rq):
    try:
        id_koma = rq.args.get('id_koma')
        environment = rq.args.get('env')
        key = rq.args.get('key')
        response_object, response_code = publish_hesderim_api.nativDeleteRaster(id_koma, environment, key)

        return create_response(response_object )

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


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8000,debug=True)
